/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.bpmn.gateway;

import io.camunda.zeebe.engine.processing.bpmn.BpmnElementContext;
import io.camunda.zeebe.engine.processing.bpmn.BpmnElementProcessor;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnBehaviors;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnIncidentBehavior;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnStateTransitionBehavior;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableFlowNode;
import io.camunda.zeebe.engine.processing.streamprocessor.sideeffect.SideEffects;
import io.camunda.zeebe.util.buffer.BufferUtil;

public final class ParallelGatewayProcessor implements BpmnElementProcessor<ExecutableFlowNode> {

  private final BpmnStateTransitionBehavior stateTransitionBehavior;
  private final BpmnIncidentBehavior bpmnIncidentBehavior;

  public ParallelGatewayProcessor(
      final BpmnBehaviors behaviors, final BpmnStateTransitionBehavior stateTransitionBehavior) {
    this.stateTransitionBehavior = stateTransitionBehavior;
    bpmnIncidentBehavior = behaviors.incidentBehavior();
  }

  @Override
  public Class<ExecutableFlowNode> getType() {
    return ExecutableFlowNode.class;
  }

  @Override
  public void onActivate(
      final ExecutableFlowNode element,
      final BpmnElementContext context,
      final SideEffects sideEffects,
      final SideEffects sideEffectQueue) {
    // the joining of the incoming sequence flows into the parallel gateway happens in the
    // sequence flow processor. The activating event of the parallel gateway is written when all
    // incoming sequence flows are taken
    final var activated = stateTransitionBehavior.transitionToActivated(context);
    final var completing = stateTransitionBehavior.transitionToCompleting(activated);
    stateTransitionBehavior
        .transitionToCompleted(element, completing)
        .ifRightOrLeft(
            completed ->
                // fork the process processing by taking all outgoing sequence flows of the parallel
                // gateway
                stateTransitionBehavior.takeOutgoingSequenceFlows(element, completed),
            failure -> bpmnIncidentBehavior.createIncident(failure, completing));
  }

  @Override
  public void onComplete(
      final ExecutableFlowNode element,
      final BpmnElementContext context,
      final SideEffects sideEffects) {
    throw new UnsupportedOperationException(
        String.format(
            "Expected to explicitly process complete, but gateway %s has already been completed on processing activate",
            BufferUtil.bufferAsString(context.getElementId())));
  }

  @Override
  public void onTerminate(
      final ExecutableFlowNode element,
      final BpmnElementContext context,
      final SideEffects sideEffects) {
    final var terminated = stateTransitionBehavior.transitionToTerminated(context);
    stateTransitionBehavior.onElementTerminated(element, terminated);
  }
}
