/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.db.impl.rocksdb.transaction;

import static org.rocksdb.Status.Code.Aborted;
import static org.rocksdb.Status.Code.Busy;
import static org.rocksdb.Status.Code.Expired;
import static org.rocksdb.Status.Code.IOError;
import static org.rocksdb.Status.Code.MergeInProgress;
import static org.rocksdb.Status.Code.Ok;
import static org.rocksdb.Status.Code.TimedOut;
import static org.rocksdb.Status.Code.TryAgain;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.util.EnumSet;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksObject;
import org.rocksdb.Status;
import org.rocksdb.Status.Code;
import org.rocksdb.Transaction;

public final class RocksDbInternal {

  static final EnumSet<Code> RECOVERABLE_ERROR_CODES =
      EnumSet.of(Ok, Aborted, Expired, IOError, Busy, TimedOut, TryAgain, MergeInProgress);

  static Field nativeHandle;
  static MethodHandle putWithHandle;
  static MethodHandle getWithHandle;
  static MethodHandle removeWithHandle;

  static {
    RocksDB.loadLibrary();

    try {
      resolveInternalMethods();
    } catch (final Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private static void resolveInternalMethods()
      throws NoSuchFieldException, NoSuchMethodException, IllegalAccessException {
    final var privateLookup =
        MethodHandles.privateLookupIn(Transaction.class, MethodHandles.lookup());

    nativeHandles();

    putWithHandle(privateLookup);
    getWithHandle(privateLookup);
    removeWithHandle(privateLookup);
  }

  private static void nativeHandles() throws NoSuchFieldException {
    nativeHandle = RocksObject.class.getDeclaredField("nativeHandle_");
    nativeHandle.setAccessible(true);
  }

  //    private native void put(final long handle, final byte[] key,
  //      final int keyLength, final byte[] value, final int valueLength,
  //      final long columnFamilyHandle)

  private static void putWithHandle(final Lookup lookup) throws NoSuchMethodException {
    try {
      putWithHandle =
          lookup.findVirtual(
              Transaction.class,
              "put",
              MethodType.methodType(
                  Void.TYPE,
                  Long.TYPE,
                  byte[].class,
                  Integer.TYPE,
                  byte[].class,
                  Integer.TYPE,
                  Long.TYPE,
                  Boolean.TYPE));
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static void getWithHandle(Lookup lookup)
      throws NoSuchMethodException, IllegalAccessException {
    getWithHandle =
        lookup.findVirtual(
            Transaction.class,
            "get",
            MethodType.methodType(
                byte[].class, Long.TYPE, Long.TYPE, byte[].class, Integer.TYPE, Long.TYPE));
  }

  private static void removeWithHandle(Lookup lookup)
      throws NoSuchMethodException, IllegalAccessException {
    removeWithHandle =
        lookup.findVirtual(
            Transaction.class,
            "delete",
            MethodType.methodType(
                Void.TYPE, Long.TYPE, byte[].class, Integer.TYPE, Long.TYPE, Boolean.TYPE));
  }

  static boolean isRocksDbExceptionRecoverable(final RocksDBException rdbex) {
    final Status status = rdbex.getStatus();
    return RECOVERABLE_ERROR_CODES.contains(status.getCode());
  }
}
