package io.kestra.webserver.services;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.async.AsyncOperationProcessedEvent;
import io.kestra.core.async.AsyncOperationProcessedEvent.Outcome;
import io.kestra.core.async.AsyncOperationsConfiguration;
import io.kestra.core.models.executions.ExecutionKilled;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.TriggerId;
import io.kestra.core.queues.BroadcastQueueInterface;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.repositories.TriggerRepositoryInterface;
import io.kestra.core.scheduler.events.CreateBackfillTrigger;
import io.kestra.core.scheduler.model.TriggerState;
import io.kestra.core.scheduler.model.TriggerType;
import io.kestra.core.scheduler.queue.TriggerEventQueue;
import io.kestra.core.server.AsyncOperationListener;
import io.kestra.core.services.AsyncOperationWaiter;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.core.trigger.Schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TriggerStateServiceTest {
    private static final String TENANT = "main";
    private static final String NAMESPACE = "io.kestra.tests";
    private static final String FLOW_ID = "flow";
    private static final String TRIGGER_ID = "my-trigger";

    private TriggerRepositoryInterface triggerRepository;
    private FlowRepositoryInterface flowRepository;
    private TriggerEventQueue triggerEventQueue;
    private AsyncOperationWaiter asyncOperationWaiter;
    private RecordingAsyncOperationListener listener;
    private TriggerStateService triggerStateService;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        triggerRepository = mock(TriggerRepositoryInterface.class);
        flowRepository = mock(FlowRepositoryInterface.class);
        triggerEventQueue = mock(TriggerEventQueue.class);
        BroadcastQueueInterface<ExecutionKilled> executionKilledQueue = mock(BroadcastQueueInterface.class);
        asyncOperationWaiter = mock(AsyncOperationWaiter.class);
        listener = new RecordingAsyncOperationListener();

        AbstractTrigger trigger = mock(AbstractTrigger.class);
        when(trigger.getId()).thenReturn(TRIGGER_ID);
        Flow flow = mock(Flow.class);
        when(flow.getTriggers()).thenReturn(List.of(trigger));
        when(flowRepository.findById(TENANT, NAMESPACE, FLOW_ID)).thenReturn(Optional.of(flow));

        triggerStateService = new TriggerStateService(
            triggerRepository,
            flowRepository,
            triggerEventQueue,
            executionKilledQueue,
            asyncOperationWaiter,
            new AsyncOperationsConfiguration(Duration.ofSeconds(5)),
            List.of(listener)
        );
    }

    @Test
    void shouldNotifyListenerOnceWhenUnlockingTriggersInBulk() {
        TriggerId triggerId = TriggerId.of(TENANT, NAMESPACE, FLOW_ID, TRIGGER_ID);
        when(triggerRepository.findByIdWithoutAcl(triggerId)).thenReturn(Optional.of(lockedTriggerState(triggerId)));

        triggerStateService.unlockAllByIds(List.of(triggerId));

        assertThat(listener.calls).hasSize(1);
        assertThat(listener.calls.getFirst().operationDescription()).isEqualTo("unlock");
        assertThat(listener.calls.getFirst().itemCount()).isEqualTo(1);
    }

    @Test
    void shouldNotNotifyListenerWhenUnlockingASingleTrigger() throws Exception {
        TriggerId triggerId = TriggerId.of(TENANT, NAMESPACE, FLOW_ID, TRIGGER_ID);
        when(triggerRepository.findByIdWithoutAcl(triggerId)).thenReturn(Optional.of(lockedTriggerState(triggerId)));
        when(asyncOperationWaiter.submitAndWait(any(), any(), any())).thenReturn(
            new AsyncOperationProcessedEvent(IdUtils.create(), TENANT, triggerId.uid(), Outcome.SUCCEEDED, null, Instant.now())
        );

        triggerStateService.unlockTriggerById(triggerId);

        assertThat(listener.calls).isEmpty();
    }

    @Test
    void shouldNotifyListenerWhenCreatingABackfill() throws Exception {
        TriggerId triggerId = TriggerId.of(TENANT, NAMESPACE, FLOW_ID, TRIGGER_ID);
        when(triggerRepository.findByIdWithoutAcl(triggerId)).thenReturn(Optional.of(scheduleTriggerState(triggerId)));
        String operationId = IdUtils.create();
        when(asyncOperationWaiter.submitAndWait(any(), any(), any())).thenReturn(
            new AsyncOperationProcessedEvent(operationId, TENANT, triggerId.uid(), Outcome.SUCCEEDED, null, Instant.now())
        );

        triggerStateService.createBackfill(
            triggerId,
            new CreateBackfillTrigger.Backfill(ZonedDateTime.now().minusDays(1), null, Map.of(), List.of())
        );

        assertThat(listener.calls).hasSize(1);
        assertThat(listener.calls.getFirst().operationId()).isNotBlank();
        assertThat(listener.calls.getFirst().operationDescription()).isEqualTo("create-backfill");
        assertThat(listener.calls.getFirst().itemCount()).isEqualTo(1);
    }

    @Test
    void shouldTagTheSentCommandWithADifferentOperationIdThanTheOneReportedToListeners() throws Exception {
        // The command's operationId only acks acceptance of the create-backfill request (the blocking
        // HTTP wait); the progressOperationId is what per-execution completions will later report
        // against. Reusing one id for both would double-count against the expected item total.
        TriggerId triggerId = TriggerId.of(TENANT, NAMESPACE, FLOW_ID, TRIGGER_ID);
        when(triggerRepository.findByIdWithoutAcl(triggerId)).thenReturn(Optional.of(scheduleTriggerState(triggerId)));
        when(asyncOperationWaiter.submitAndWait(any(), any(), any())).thenAnswer(invocation ->
        {
            Consumer<String> submit = invocation.getArgument(1);
            submit.accept(IdUtils.create());
            return new AsyncOperationProcessedEvent(IdUtils.create(), TENANT, triggerId.uid(), Outcome.SUCCEEDED, null, Instant.now());
        });

        triggerStateService.createBackfill(
            triggerId,
            new CreateBackfillTrigger.Backfill(ZonedDateTime.now().minusDays(1), null, Map.of(), List.of())
        );

        ArgumentCaptor<CreateBackfillTrigger> captor = ArgumentCaptor.forClass(CreateBackfillTrigger.class);
        verify(triggerEventQueue).send(captor.capture());
        CreateBackfillTrigger sent = captor.getValue();

        assertThat(sent.progressOperationId()).isEqualTo(listener.calls.getFirst().operationId());
        assertThat(sent.operationId()).isNotEqualTo(sent.progressOperationId());
    }

    @Test
    void shouldNotifyListenerWithExpectedExecutionCountWhenCreatingABoundedBackfill() throws Exception {
        TriggerId triggerId = TriggerId.of(TENANT, NAMESPACE, FLOW_ID, TRIGGER_ID);
        when(triggerRepository.findByIdWithoutAcl(triggerId)).thenReturn(Optional.of(scheduleTriggerState(triggerId)));
        when(asyncOperationWaiter.submitAndWait(any(), any(), any())).thenReturn(
            new AsyncOperationProcessedEvent(IdUtils.create(), TENANT, triggerId.uid(), Outcome.SUCCEEDED, null, Instant.now())
        );
        Schedule schedule = Schedule.builder().id(TRIGGER_ID).type(Schedule.class.getName()).cron("* * * * *").build();
        Flow flow = mock(Flow.class);
        when(flow.getTriggers()).thenReturn(List.of(schedule));
        when(flowRepository.findById(TENANT, NAMESPACE, FLOW_ID)).thenReturn(Optional.of(flow));

        ZonedDateTime start = ZonedDateTime.now().truncatedTo(ChronoUnit.MINUTES);
        ZonedDateTime end = start.plusMinutes(4);

        triggerStateService.createBackfill(triggerId, new CreateBackfillTrigger.Backfill(start, end, Map.of(), List.of()));

        assertThat(listener.calls).hasSize(1);
        assertThat(listener.calls.getFirst().itemCount()).isEqualTo(5);
    }

    private static TriggerState lockedTriggerState(TriggerId triggerId) {
        return TriggerState.builder()
            .tenantId(triggerId.getTenantId())
            .namespace(triggerId.getNamespace())
            .flowId(triggerId.getFlowId())
            .triggerId(triggerId.getTriggerId())
            .locked(true)
            .type(TriggerType.POLLING)
            .build();
    }

    private static TriggerState scheduleTriggerState(TriggerId triggerId) {
        return TriggerState.builder()
            .tenantId(triggerId.getTenantId())
            .namespace(triggerId.getNamespace())
            .flowId(triggerId.getFlowId())
            .triggerId(triggerId.getTriggerId())
            .type(TriggerType.SCHEDULE)
            .build();
    }

    private record ListenerCall(String operationId, String operationDescription, int itemCount) {
    }

    private static class RecordingAsyncOperationListener implements AsyncOperationListener {
        private final List<ListenerCall> calls = new ArrayList<>();

        @Override
        public void onAsyncOperationCreated(String operationId, String operationDescription, int itemCount) {
            calls.add(new ListenerCall(operationId, operationDescription, itemCount));
        }
    }
}
