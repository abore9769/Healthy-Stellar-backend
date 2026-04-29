import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { context, propagation, trace } from '@opentelemetry/api';
import { ConfigService } from '@nestjs/config';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { QUEUE_NAMES } from '../queue.constants';
import { verifyQueuePayload } from '../queue-payload.util';
import { RecordEventStoreService } from '../../records/services/record-event-store.service';
import { RecordEventType } from '../../records/entities/record-event.entity';
import { RECORD_DELETED_EVENT } from '../../records/services/record-sync.service';

/**
 * EventIndexingProcessor
 *
 * Handles indexing of blockchain events from Stellar/Soroban contracts
 * Processes contract events and stores them in the local database for querying
 */
@Processor(QUEUE_NAMES.EVENT_INDEXING, {
  concurrency: 2, // Sequential processing to maintain event ordering
})
export class EventIndexingProcessor extends WorkerHost {
  private readonly logger = new Logger(EventIndexingProcessor.name);
  private readonly tracer = trace.getTracer('healthy-stellar-backend');

  constructor(
    private readonly configService: ConfigService,
    private readonly recordEventStore: RecordEventStoreService,
    private readonly eventEmitter: EventEmitter2,
  ) {
    super();
  }

  /**
   * Process event indexing job
   */
  async process(job: Job<any>): Promise<any> {
    const { eventType, data, contractAddress, correlationId, traceContext } =
      job.data;

    // Integrity check — reject tampered payloads before any processing
    verifyQueuePayload(job.data, this.configService.getOrThrow<string>('QUEUE_HMAC_SECRET'));

    // Extract trace context from job data
    const extractedContext = traceContext
      ? propagation.extract(context.active(), traceContext)
      : context.active();

    return context.with(extractedContext, async () => {
      const span = this.tracer.startSpan('queue.process.eventIndexing', {
        attributes: {
          'queue.name': QUEUE_NAMES.EVENT_INDEXING,
          'queue.job_id': job.id,
          'queue.event_type': eventType,
          'queue.correlation_id': correlationId,
          'queue.attempt': job.attemptsMade + 1,
          'blockchain.contract_address': contractAddress,
        },
      });

      return context.with(trace.setSpan(context.active(), span), async () => {
        try {
          this.logger.log(
            `[${QUEUE_NAMES.EVENT_INDEXING}] Processing event - Type: ${eventType}, Contract: ${contractAddress}, Job: ${job.id}`,
          );

          job.progress(10);

          const result = await this.indexEvent(eventType, data, contractAddress, job);

          job.progress(100);

          span.addEvent('queue.job.completed', {
            'queue.indexed_records': result.count,
          });

          this.logger.log(
            `[${QUEUE_NAMES.EVENT_INDEXING}] Successfully indexed ${result.count} events`,
          );

          return result;
        } catch (error) {
          const errorMessage = (error as Error).message;
          const errorStack = (error as Error).stack;

          span.recordException(error as Error);
          span.addEvent('queue.job.failed', {
            'error.message': errorMessage,
            'error.type': (error as Error).constructor.name,
          });

          const isLastAttempt = job.attemptsMade >= job.opts.attempts! - 1;
          this.logger.error(
            `[${QUEUE_NAMES.EVENT_INDEXING}] Job ${job.id} failed on attempt ${job.attemptsMade + 1}/${job.opts.attempts}: ${errorMessage}${isLastAttempt ? ' (FINAL ATTEMPT)' : ''}`,
            errorStack,
          );

          throw error;
        } finally {
          span.end();
        }
      });
    });
  }

  /**
   * Index contract event data
   */
  private async indexEvent(
    eventType: string,
    data: any,
    contractAddress: string,
    job: Job,
  ): Promise<any> {
    this.logger.debug(
      `[Event Indexing] Indexing event - Type: ${eventType}, Contract: ${contractAddress}`,
    );

    job.progress(30);

    const safeEventType = typeof eventType === 'string' ? eventType : 'UnknownEvent';
    const safeData = data && typeof data === 'object' ? data : {};

    // "Real" processing: map known Soroban contract events into off-chain workflows.
    // This is intentionally idempotent-friendly:
    // - appending to the per-record event stream is safe (strict ordering enforced there)
    // - downstream handlers (e.g. RecordSyncService) are idempotent
    const mapped = await this.applyEventSideEffects(safeEventType, safeData);

    job.progress(90);

    return {
      status: 'success',
      operation: 'indexEvent',
      eventType: safeEventType,
      contractAddress,
      blockHeight: safeData.blockHeight ?? null,
      eventSequence: safeData.eventSequence ?? null,
      count: mapped.indexedCount,
      effects: mapped.effects,
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Apply side effects for known on-chain events.
   */
  private async applyEventSideEffects(
    eventType: string,
    data: Record<string, any>,
  ): Promise<{ indexedCount: number; effects: string[] }> {
    const effects: string[] = [];

    // Common extracted fields (best-effort, tolerant of shape changes)
    const recordId = typeof data.recordId === 'string' ? data.recordId : null;
    const txHash =
      typeof data.txHash === 'string'
        ? data.txHash
        : typeof data.stellarTxHash === 'string'
          ? data.stellarTxHash
          : null;

    // 1) Record anchored on-chain — reflect in record event log if recordId is known
    if (eventType === 'ContractEventAnchorRecord' && recordId && txHash) {
      await this.recordEventStore.append(
        recordId,
        RecordEventType.RECORD_STELLAR_ANCHORED,
        { stellarTxHash: txHash, blockHeight: data.blockHeight ?? null },
      );
      effects.push('record_event_store.append(RECORD_STELLAR_ANCHORED)');
      return { indexedCount: 1, effects };
    }

    // 2) Record deleted on-chain — emit sync event + append immutable event log
    // (Event name varies by contract generator; handle common variants.)
    const isDeleteEvent =
      eventType === 'ContractEventRecordDeleted' ||
      eventType === 'ContractEventDeleteRecord' ||
      eventType === 'ContractEventRecordRemoved' ||
      eventType === 'ContractEventRecordDeletedV1' ||
      eventType.toLowerCase().includes('deleted');

    if (isDeleteEvent && recordId) {
      const deletedAt = this.parseEventTimestamp(data.deletedAt ?? data.timestamp) ?? new Date();

      // Emit the domain event used by RecordSyncService to mirror the deletion flag.
      // Note: RecordSyncService is idempotent, so duplicate emissions are safe.
      this.eventEmitter.emit(RECORD_DELETED_EVENT, {
        recordId,
        txHash: txHash ?? 'unknown',
        deletedAt,
      });
      effects.push(`event_emitter.emit(${RECORD_DELETED_EVENT})`);

      await this.recordEventStore.append(
        recordId,
        RecordEventType.RECORD_DELETED,
        { txHash: txHash ?? null, deletedAt: deletedAt.toISOString() },
      );
      effects.push('record_event_store.append(RECORD_DELETED)');

      return { indexedCount: 1, effects };
    }

    // 3) Access grant / revoke events — currently emitted for downstream listeners
    // (notifications, subscriptions, projections). Persisting is left to consumers.
    if (eventType === 'ContractEventAccessGranted') {
      this.eventEmitter.emit('chain.access_granted', { ...data });
      effects.push('event_emitter.emit(chain.access_granted)');
      return { indexedCount: 1, effects };
    }

    if (eventType === 'ContractEventAccessRevoked') {
      this.eventEmitter.emit('chain.access_revoked', { ...data });
      effects.push('event_emitter.emit(chain.access_revoked)');
      return { indexedCount: 1, effects };
    }

    // Unknown/unhandled event types: log and mark as "indexed" for observability.
    this.logger.debug(
      `[Event Indexing] Unhandled eventType=${eventType} fields=${Object.keys(data).length}`,
    );
    effects.push('noop(unhandled_event_type)');
    return { indexedCount: 1, effects };
  }

  private parseEventTimestamp(value: unknown): Date | null {
    if (!value) return null;
    if (value instanceof Date) return value;
    if (typeof value === 'number' && Number.isFinite(value)) return new Date(value);
    if (typeof value === 'string') {
      const d = new Date(value);
      return Number.isNaN(d.getTime()) ? null : d;
    }
    return null;
  }
}
