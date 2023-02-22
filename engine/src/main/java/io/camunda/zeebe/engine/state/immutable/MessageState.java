/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.state.immutable;

import io.camunda.zeebe.engine.state.message.StoredMessage;
import org.agrona.DirectBuffer;

public interface MessageState {

  boolean existMessageCorrelation(long messageKey, DirectBuffer bpmnProcessId);

  boolean existActiveProcessInstance(DirectBuffer bpmnProcessId, DirectBuffer correlationKey);

  DirectBuffer getProcessInstanceCorrelationKey(long processInstanceKey);

  void visitMessages(DirectBuffer name, DirectBuffer correlationKey, MessageVisitor visitor);

  StoredMessage getMessage(long messageKey);

  /**
   * @param timestamp
   * @param startAt
   * @param visitor
   */
  void visitMessagesWithDeadlineBeforeTimestamp(
      long timestamp, final Index startAt, ExpiredMessageVisitor visitor);

  boolean exist(DirectBuffer name, DirectBuffer correlationKey, DirectBuffer messageId);

  /**
   * @param key
   * @param deadline
   */
  record Index(long key, long deadline) {}

  @FunctionalInterface
  interface MessageVisitor {
    boolean visit(StoredMessage message);
  }

  @FunctionalInterface
  interface ExpiredMessageVisitor {
    boolean visit(final long deadline, long messageKey);
  }
}
