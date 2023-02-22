/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.message;

import io.camunda.zeebe.engine.api.ProcessingScheduleService;
import io.camunda.zeebe.engine.api.Task;
import io.camunda.zeebe.engine.api.TaskResult;
import io.camunda.zeebe.engine.api.TaskResultBuilder;
import io.camunda.zeebe.engine.state.immutable.MessageState;
import io.camunda.zeebe.protocol.impl.record.value.message.MessageRecord;
import io.camunda.zeebe.protocol.record.intent.MessageIntent;
import io.camunda.zeebe.scheduler.clock.ActorClock;
import java.time.Duration;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableInteger;

/**
 * The Message TTL Checker looks for expired message deadlines, and for each of those it writes an
 * EXPIRE Message command.
 *
 * <p>To prevent that it clogs the log stream with too many EXPIRE Message commands, it only writes
 * a limited number of these commands in a single run of {@link #execute(TaskResultBuilder)}.
 *
 * <p>It determines whether to reschedule itself immediately, or after the configured {@link
 * MessageObserver#MESSAGE_TIME_TO_LIVE_CHECK_INTERVAL interval}. If it reschedules itself
 * immediately, then it will continue where it left off the last time. Otherwise, it starts with the
 * first expired message deadline it can find.
 */
public final class MessageTimeToLiveChecker implements Task {

  private static final MessageRecord EMPTY_DELETE_MESSAGE_COMMAND =
      new MessageRecord().setName("").setCorrelationKey("").setTimeToLive(-1L);

  private static final int EXPIRE_COMMANDS_LIMIT_DEFAULT = 10;

  /** This determines the maximum number of EXPIRE commands it will attempt to fit in the result. */
  private static final int LIMIT = EXPIRE_COMMANDS_LIMIT_DEFAULT;

  private final ProcessingScheduleService scheduleService;
  private final MessageState messageState;
  /** Keeps track of where to continue between iterations. */
  private MessageState.Index lastIndex;

  public MessageTimeToLiveChecker(
      final ProcessingScheduleService scheduleService, final MessageState messageState) {
    this.scheduleService = scheduleService;
    this.messageState = messageState;
    lastIndex = null;
  }

  @Override
  public TaskResult execute(final TaskResultBuilder taskResultBuilder) {
    final var counter = new MutableInteger(0);
    final var isBatchFull = new MutableBoolean(false);

    /*
     * After visiting the messages with expired deadlines, we need to determine whether to restart
     * from the beginning or to continue where we left off. The visit method doesn't inform us
     * whether we reached the actual end of all expired message deadlines, or that the iteration was
     * stopped by us. So, we need to determine it using what we know.
     *
     * There are 4 iteration scenarios:
     *  - the command does not fit anymore
     *    -> we continue where we left off in another iteration
     *  - the command fits, but we reached the configured limit
     *    -> we continue where we left off in another iteration
     *  - the command fits, and we didn't reach the configured limit yet
     *    -> continue this iteration
     *  - the iteration finished, didn't reach the configured limit, and the last command fitted
     *    -> restart from the beginning after the configured interval
     */
    messageState.visitMessagesWithDeadlineBeforeTimestamp(
        ActorClock.currentTimeMillis(),
        lastIndex,
        (deadline, expiredMessageKey) -> {
          lastIndex = new MessageState.Index(expiredMessageKey, deadline);

          final var stillFitsInResult =
              taskResultBuilder.appendCommandRecord(
                  expiredMessageKey, MessageIntent.EXPIRE, EMPTY_DELETE_MESSAGE_COMMAND);

          if (!stillFitsInResult) {
            // we can stop this iteration and continue where we left off in another iteration
            isBatchFull.set(true);
            return false;
          }

          // if we reach the limit, we can stop this iteration and continue where we left off in
          // another iteration
          return counter.incrementAndGet() <= LIMIT;
        });

    // The visitor doesn't inform whether we reached the actual end of all expired message
    // deadlines, or that the iteration was stopped by us. We need to determine whether to restart
    // from the beginning or to continue where we left off, using what we know.
    final boolean shouldContinueWhereLeftOff = counter.get() > LIMIT || isBatchFull.get();
    reschedule(shouldContinueWhereLeftOff);

    return taskResultBuilder.build();
  }

  private void reschedule(final boolean shouldContinueWhereLeftOff) {
    final Duration duration;
    if (shouldContinueWhereLeftOff) {
      duration = Duration.ZERO;
    } else {
      lastIndex = null;
      duration = MessageObserver.MESSAGE_TIME_TO_LIVE_CHECK_INTERVAL;
    }

    scheduleService.runDelayed(duration, this);
  }
}
