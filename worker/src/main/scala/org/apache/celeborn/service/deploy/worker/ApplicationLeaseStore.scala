/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.celeborn.service.deploy.worker

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, EOFException, File, FileInputStream, FileOutputStream, IOException}
import java.nio.channels.FileChannel
import java.nio.file.{AtomicMoveNotSupportedException, Files, StandardCopyOption, StandardOpenOption}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.celeborn.common.meta.ApplicationLease

/** Crash-durable application fencing epochs installed on a worker. */
final private[worker] class ApplicationLeaseStore(root: File, durableIdentity: Boolean = true)
  extends AutoCloseable {
  private val Magic = 0x434C5345 // "CLSE"
  private val Version = 1
  private val leases = new ConcurrentHashMap[String, ApplicationLease]()
  private val stateFile = new File(root, "application-leases.bin")

  if (!root.isDirectory && !root.mkdirs()) {
    throw new IOException(s"Unable to create worker lease directory $root")
  }
  load()

  def current(appId: String): ApplicationLease = leases.get(appId)

  /** Installs an acquisition or renewal and makes it durable before returning. */
  def install(appId: String, lease: ApplicationLease): ApplicationLease = synchronized {
    if (!durableIdentity) {
      throw new IllegalStateException(
        "Application fencing requires a configured non-zero worker RPC port")
    }
    require(appId != null && appId.nonEmpty, "Application id must be non-empty")
    require(lease != null, "Application lease must be non-null")
    val previous = leases.get(appId)
    if (previous != null) {
      if (lease.epoch() < previous.epoch()) {
        throw new IllegalStateException(
          s"Application $appId is fenced at epoch ${previous.epoch()}, not ${lease.epoch()}")
      }
      if (lease.epoch() == previous.epoch() && lease.ownerId() != previous.ownerId()) {
        throw new IllegalStateException(
          s"Application $appId epoch ${lease.epoch()} belongs to ${previous.ownerId()}")
      }
      if (lease.epoch() == previous.epoch() && lease.expiresAtMs() < previous.expiresAtMs()) {
        throw new IllegalArgumentException("Application lease renewal cannot shorten expiry")
      }
      if (lease == previous) return previous
    }
    leases.put(appId, lease)
    try persist()
    catch {
      case e: Throwable =>
        if (previous == null) leases.remove(appId) else leases.put(appId, previous)
        throw e
    }
    lease
  }

  /** Legacy epoch zero is accepted only until this worker has installed a lease for the app. */
  def validate(appId: String, epoch: Long, ownerId: String, nowMs: Long): Unit = {
    val lease = leases.get(appId)
    if (lease == null) {
      if (epoch != 0L || (ownerId != null && ownerId.nonEmpty)) {
        throw new IllegalStateException(s"No application lease is installed for $appId")
      }
    } else if (!lease.isValid(epoch, ownerId, nowMs)) {
      throw new IllegalStateException(
        s"Application lease is not valid for $appId at epoch $epoch and owner $ownerId")
    }
  }

  private def load(): Unit = synchronized {
    if (!stateFile.exists()) return
    val in = new DataInputStream(new BufferedInputStream(new FileInputStream(stateFile)))
    try {
      if (in.readInt() != Magic || in.readInt() != Version) {
        throw new IOException(s"Unsupported worker lease state in $stateFile")
      }
      val count = in.readInt()
      if (count < 0) throw new IOException(s"Negative lease count in $stateFile")
      (0 until count).foreach { _ =>
        val appId = in.readUTF()
        val lease = new ApplicationLease(in.readLong(), in.readUTF(), in.readLong())
        if (appId.isEmpty || leases.putIfAbsent(appId, lease) != null) {
          throw new IOException(s"Invalid duplicate application lease for $appId")
        }
      }
      if (in.read() != -1) throw new IOException(s"Trailing bytes in worker lease state $stateFile")
    } catch {
      case e: EOFException => throw new IOException(s"Truncated worker lease state $stateFile", e)
    } finally in.close()
  }

  private def persist(): Unit = {
    val temporary = new File(root, stateFile.getName + ".tmp")
    val fileOut = new FileOutputStream(temporary)
    val out = new DataOutputStream(new BufferedOutputStream(fileOut))
    try {
      out.writeInt(Magic)
      out.writeInt(Version)
      val snapshot = leases.asScala.toSeq.sortBy(_._1)
      out.writeInt(snapshot.size)
      snapshot.foreach { case (appId, lease) =>
        out.writeUTF(appId)
        out.writeLong(lease.epoch())
        out.writeUTF(lease.ownerId())
        out.writeLong(lease.expiresAtMs())
      }
      out.flush()
      fileOut.getFD.sync()
    } finally out.close()
    try {
      Files.move(
        temporary.toPath,
        stateFile.toPath,
        StandardCopyOption.ATOMIC_MOVE,
        StandardCopyOption.REPLACE_EXISTING)
    } catch {
      case _: AtomicMoveNotSupportedException =>
        Files.move(temporary.toPath, stateFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    }
    val directory = FileChannel.open(root.toPath, StandardOpenOption.READ)
    try directory.force(true) finally directory.close()
  }

  override def close(): Unit = {}
}
