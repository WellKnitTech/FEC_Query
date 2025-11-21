import { useState, useEffect, useRef, useCallback } from 'react';
import { bulkDataApi, BulkImportJobStatus } from '../services/api';

interface UseMultipleJobTrackingReturn {
  jobs: Map<string, BulkImportJobStatus>;
  pendingJobs: Set<string>;
  startTracking: (jobId: string) => void;
  stopTracking: (jobId: string) => void;
  cancelJob: (jobId: string) => Promise<void>;
  loadJobs: (jobs: BulkImportJobStatus[]) => void;
}

/**
 * Custom hook for tracking multiple bulk import jobs via WebSocket or polling
 * @param onJobComplete - Optional callback when a job completes or fails
 */
export function useMultipleJobTracking(
  onJobComplete?: () => void
): UseMultipleJobTrackingReturn {
  const [jobs, setJobs] = useState<Map<string, BulkImportJobStatus>>(new Map());
  const [pendingJobs, setPendingJobs] = useState<Set<string>>(new Set());
  const wsRefs = useRef<Map<string, WebSocket>>(new Map());
  const pollingIntervals = useRef<Map<string, NodeJS.Timeout>>(new Map());

  const stopTracking = useCallback((jobId: string) => {
    // Close WebSocket if open
    const ws = wsRefs.current.get(jobId);
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.close();
    }
    wsRefs.current.delete(jobId);

    // Clear polling interval
    const interval = pollingIntervals.current.get(jobId);
    if (interval) {
      clearInterval(interval);
      pollingIntervals.current.delete(jobId);
    }

    // Remove from pending
    setPendingJobs((prev) => {
      const updated = new Set(prev);
      updated.delete(jobId);
      return updated;
    });
  }, []);

  const startPolling = useCallback((jobId: string) => {
    // Stop any existing polling for this job
    if (pollingIntervals.current.has(jobId)) {
      clearInterval(pollingIntervals.current.get(jobId)!);
    }

    const poll = async () => {
      try {
        const jobStatus = await bulkDataApi.getJobStatus(jobId);
        setJobs((prev) => {
          const updated = new Map(prev);
          updated.set(jobId, jobStatus);
          return updated;
        });

        // Remove from pending once we have status
        setPendingJobs((prev) => {
          const updated = new Set(prev);
          updated.delete(jobId);
          return updated;
        });

        // Stop polling if job is done
        if (jobStatus.status !== 'running' && jobStatus.status !== 'pending') {
          stopTracking(jobId);
          if (jobStatus.status === 'completed' || jobStatus.status === 'failed') {
            onJobComplete?.();
          }
        }
      } catch (err) {
        // Error polling job
      }
    };

    // Poll immediately, then every 2 seconds
    poll();
    const interval = setInterval(poll, 2000);
    pollingIntervals.current.set(jobId, interval);
  }, [stopTracking, onJobComplete]);

  const startTracking = useCallback((jobId: string) => {
    // Check if already tracking
    if (wsRefs.current.has(jobId) || pollingIntervals.current.has(jobId)) {
      return; // Already tracking
    }

    // Add to pending jobs immediately
    setPendingJobs((prev) => new Set(prev).add(jobId));

    // First, try to get current status immediately
    bulkDataApi
      .getJobStatus(jobId)
      .then((jobStatus) => {
        setJobs((prev) => {
          const updated = new Map(prev);
          updated.set(jobId, jobStatus);
          return updated;
        });

        // Remove from pending once we have status
        setPendingJobs((prev) => {
          const updated = new Set(prev);
          updated.delete(jobId);
          return updated;
        });

        // Only start WebSocket/polling if job is still active
        if (jobStatus.status === 'running' || jobStatus.status === 'pending') {
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
                const updatedJobStatus = message.data as BulkImportJobStatus;
                setJobs((prev) => {
                  const updated = new Map(prev);
                  updated.set(jobId, updatedJobStatus);
                  return updated;
                });

                // Remove from pending once we have status
                setPendingJobs((prev) => {
                  const updated = new Set(prev);
                  updated.delete(jobId);
                  return updated;
                });

                // Stop tracking if job is done
                if (message.type !== 'progress') {
                  stopTracking(jobId);
                  if (message.type === 'completed' || message.type === 'failed') {
                    onJobComplete?.();
                  }
                }
              }
            },
            () => {
              // WebSocket failed, falling back to polling
              startPolling(jobId);
            }
          );

          if (ws) {
            wsRefs.current.set(jobId, ws);

            // Fallback to polling if WebSocket closes unexpectedly
            ws.onclose = () => {
              if (!pollingIntervals.current.has(jobId)) {
                startPolling(jobId);
              }
            };
          } else {
            // WebSocket creation failed, use polling
            startPolling(jobId);
          }
        } else {
          // Job is already done, no need to track
          stopTracking(jobId);
        }
      })
      .catch((err) => {
        // Still try to start tracking even if initial fetch fails
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
              const updatedJobStatus = message.data as BulkImportJobStatus;
              setJobs((prev) => {
                const updated = new Map(prev);
                updated.set(jobId, updatedJobStatus);
                return updated;
              });

              // Remove from pending once we have status
              setPendingJobs((prev) => {
                const updated = new Set(prev);
                updated.delete(jobId);
                return updated;
              });

              // Stop tracking if job is done
              if (message.type !== 'progress') {
                stopTracking(jobId);
                if (message.type === 'completed' || message.type === 'failed') {
                  onJobComplete?.();
                }
              }
            }
          },
          () => {
            // WebSocket failed, falling back to polling
            startPolling(jobId);
          }
        );

        if (ws) {
          wsRefs.current.set(jobId, ws);

          // Fallback to polling if WebSocket closes unexpectedly
          ws.onclose = () => {
            if (!pollingIntervals.current.has(jobId)) {
              startPolling(jobId);
            }
          };
        } else {
          // WebSocket creation failed, use polling
          startPolling(jobId);
        }
      });
  }, [startPolling, stopTracking, onJobComplete]);

  // Load jobs into state and start tracking active ones
  const loadJobsWithTracking = useCallback((jobsToLoad: BulkImportJobStatus[]) => {
    setJobs((prev) => {
      const updated = new Map(prev);
      for (const job of jobsToLoad) {
        updated.set(job.job_id, job);
      }
      return updated;
    });

    // Start tracking active jobs after state update
    for (const job of jobsToLoad) {
      if (job.status === 'running' || job.status === 'pending') {
        // Only start tracking if not already tracking
        if (!wsRefs.current.has(job.job_id) && !pollingIntervals.current.has(job.job_id)) {
          // Use setTimeout to avoid calling startTracking during state update
          setTimeout(() => startTracking(job.job_id), 0);
        }
      }
    }
  }, [startTracking]);

  const cancelJob = useCallback(async (jobId: string) => {
    try {
      await bulkDataApi.cancelJob(jobId);
      // Job status will be updated via WebSocket/polling
    } catch (err) {
      throw err;
    }
  }, []);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      wsRefs.current.forEach((ws) => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.close();
        }
      });
      pollingIntervals.current.forEach((interval) => clearInterval(interval));
    };
  }, []);

  return {
    jobs,
    pendingJobs,
    startTracking,
    stopTracking,
    cancelJob,
    loadJobs: loadJobsWithTracking,
  };
}
