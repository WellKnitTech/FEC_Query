import { useState, useEffect, useRef, useCallback } from 'react';
import { bulkDataApi, BulkImportJobStatus } from '../services/api';

interface UseJobTrackingReturn {
  jobStatus: BulkImportJobStatus | null;
  isTracking: boolean;
  startTracking: () => void;
  stopTracking: () => void;
}

/**
 * Custom hook for tracking a single bulk import job via WebSocket or polling
 * @param jobId - The job ID to track
 * @param onStatusUpdate - Optional callback when job status updates
 */
export function useJobTracking(
  jobId: string | null,
  onStatusUpdate?: (status: BulkImportJobStatus) => void
): UseJobTrackingReturn {
  const [jobStatus, setJobStatus] = useState<BulkImportJobStatus | null>(null);
  const [isTracking, setIsTracking] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const pollingIntervalRef = useRef<NodeJS.Timeout | null>(null);

  const stopTracking = useCallback(() => {
    // Close WebSocket if open
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.close();
    }
    wsRef.current = null;

    // Clear polling interval
    if (pollingIntervalRef.current) {
      clearInterval(pollingIntervalRef.current);
      pollingIntervalRef.current = null;
    }

    setIsTracking(false);
  }, []);

  const startPolling = useCallback(() => {
    if (!jobId) return;

    // Stop any existing polling
    if (pollingIntervalRef.current) {
      clearInterval(pollingIntervalRef.current);
    }

    const poll = async () => {
      try {
        const status = await bulkDataApi.getJobStatus(jobId);
        setJobStatus(status);
        onStatusUpdate?.(status);

        // Stop polling if job is done
        if (status.status !== 'running' && status.status !== 'pending') {
          stopTracking();
        }
      } catch (err) {
        console.error(`Error polling job ${jobId}:`, err);
      }
    };

    // Poll immediately, then every 2 seconds
    poll();
    pollingIntervalRef.current = setInterval(poll, 2000);
  }, [jobId, onStatusUpdate, stopTracking]);

  const startTracking = useCallback(() => {
    if (!jobId) return;

    setIsTracking(true);

    // Try WebSocket first
    const ws = bulkDataApi.createWebSocket(
      jobId,
      (message) => {
        if (
          message.type === 'progress' ||
          message.type === 'completed' ||
          message.type === 'failed' ||
          message.type === 'cancelled'
        ) {
          // The backend sends { type, job_id, data: { ...jobStatus } }
          const status = message.data as BulkImportJobStatus;
          setJobStatus(status);
          onStatusUpdate?.(status);

          // Stop tracking if job is done
          if (message.type !== 'progress') {
            stopTracking();
          }
        }
      },
      () => {
        // WebSocket failed, falling back to polling
        startPolling();
      }
    );

    if (ws) {
      wsRef.current = ws;

      // Fallback to polling if WebSocket closes unexpectedly
      ws.onclose = () => {
        if (!pollingIntervalRef.current) {
          startPolling();
        }
      };
    } else {
      // WebSocket creation failed, use polling
      startPolling();
    }
  }, [jobId, onStatusUpdate, stopTracking, startPolling]);

  // Cleanup on unmount or jobId change
  useEffect(() => {
    return () => {
      stopTracking();
    };
  }, [stopTracking]);

  // Auto-start tracking if jobId is provided
  useEffect(() => {
    if (jobId && !isTracking) {
      // First, try to get current status
      bulkDataApi
        .getJobStatus(jobId)
        .then((status) => {
          setJobStatus(status);
          onStatusUpdate?.(status);

          // Start tracking if job is still active
          if (status.status === 'running' || status.status === 'pending') {
            startTracking();
          }
        })
        .catch((err) => {
          console.error(`Error getting initial job status for ${jobId}:`, err);
        });
    }
  }, [jobId]); // Only depend on jobId, not isTracking or startTracking to avoid loops

  return {
    jobStatus,
    isTracking,
    startTracking,
    stopTracking,
  };
}

