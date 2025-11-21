import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

interface UseCachedQueryOptions<T> {
  queryKey: string;
  fetcher: (signal: AbortSignal) => Promise<T>;
  enabled?: boolean;
  staleTimeMs?: number;
}

interface CacheEntry<T> {
  data?: T;
  error?: string | null;
  updatedAt?: number;
  promise?: Promise<T>;
  controller?: AbortController;
  subscribers?: number;
}

interface UseCachedQueryResult<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
  revalidate: () => Promise<void>;
}

const DEFAULT_STALE_TIME_MS = 5 * 60 * 1000; // 5 minutes

const queryCache = new Map<string, CacheEntry<unknown>>();

function normalizeError(err: any): string {
  if (err?.name === 'AbortError') {
    return 'Request was aborted';
  }
  return err?.response?.data?.detail || err?.message || 'An unexpected error occurred';
}

function attachSubscriber(key: string): () => void {
  const entry = queryCache.get(key);
  if (!entry) {
    return () => {};
  }
  entry.subscribers = (entry.subscribers ?? 0) + 1;
  queryCache.set(key, entry);
  return () => releaseSubscriber(key);
}

function releaseSubscriber(key: string) {
  const entry = queryCache.get(key);
  if (!entry) {
    return;
  }
  entry.subscribers = Math.max((entry.subscribers ?? 1) - 1, 0);
  if (entry.subscribers === 0 && entry.promise && entry.controller && !entry.controller.signal.aborted) {
    entry.controller.abort();
  }
  queryCache.set(key, entry);
}

function updateCacheEntry<T>(key: string, updater: (entry: CacheEntry<T>) => CacheEntry<T>) {
  const current = queryCache.get(key) as CacheEntry<T> | undefined;
  const next = updater(current ?? {} as CacheEntry<T>);
  queryCache.set(key, next as CacheEntry<unknown>);
}

/**
 * A lightweight caching hook that standardizes stale-while-revalidate behavior and request deduplication.
 *
 * Usage example:
 * const { data, loading, error, refresh } = useCachedQuery({
 *   queryKey: `candidate:${candidateId}`,
 *   enabled: Boolean(candidateId),
 *   fetcher: (signal) => candidateApi.getById(candidateId!, signal),
 * });
 */
export function useCachedQuery<T>({
  queryKey,
  fetcher,
  enabled = true,
  staleTimeMs = DEFAULT_STALE_TIME_MS,
}: UseCachedQueryOptions<T>): UseCachedQueryResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState<boolean>(enabled);
  const [error, setError] = useState<string | null>(null);
  const attachedReleaseRef = useRef<(() => void) | null>(null);

  const memoizedOptions = useMemo(
    () => ({ queryKey, fetcher, enabled, staleTimeMs }),
    [queryKey, fetcher, enabled, staleTimeMs]
  );

  const runQuery = useCallback(
    async (forceRefresh = false) => {
      const { queryKey: key, fetcher: fetchFn, enabled: isEnabled, staleTimeMs: staleMs } = memoizedOptions;

      if (!isEnabled) {
        setLoading(false);
        return;
      }

      const now = Date.now();
      const cachedEntry = queryCache.get(key) as CacheEntry<T> | undefined;
      const isFresh = cachedEntry?.updatedAt ? now - cachedEntry.updatedAt < staleMs : false;
      const hasCachedData = cachedEntry?.data !== undefined;

      if (hasCachedData) {
        setData(cachedEntry!.data ?? null);
        setError(cachedEntry!.error ?? null);
        if (!forceRefresh && isFresh) {
          setLoading(false);
          return;
        }
      }

      // If a force refresh was requested, abort any in-flight request so new results win
      if (forceRefresh && cachedEntry?.controller && !cachedEntry.controller.signal.aborted) {
        cachedEntry.controller.abort();
      }

      // If there is an existing request for this key, subscribe to it instead of issuing a new one
      if (!forceRefresh && cachedEntry?.promise) {
        setLoading(true);
        const detach = attachSubscriber(key);
        attachedReleaseRef.current = detach;
        try {
          const result = await cachedEntry.promise;
          setData(result ?? null);
          setError(null);
        } catch (err) {
          setError(normalizeError(err));
        } finally {
          detach();
          attachedReleaseRef.current = null;
          setLoading(false);
        }
        return;
      }

      setLoading(true);
      const controller = new AbortController();
      const fetchPromise = (async () => {
        try {
          const result = await fetchFn(controller.signal);
          updateCacheEntry<T>(key, (entry) => ({
            ...entry,
            data: result,
            error: null,
            updatedAt: Date.now(),
          }));
          return result;
        } catch (err: any) {
          if (controller.signal.aborted) {
            throw err;
          }
          const formattedError = normalizeError(err);
          updateCacheEntry<T>(key, (entry) => ({
            ...entry,
            error: formattedError,
            updatedAt: Date.now(),
          }));
          throw err;
        } finally {
          updateCacheEntry<T>(key, (entry) => ({
            ...entry,
            promise: undefined,
            controller: undefined,
            subscribers: 0,
          }));
        }
      })();

      updateCacheEntry<T>(key, (entry) => ({
        ...entry,
        promise: fetchPromise,
        controller,
        subscribers: entry.subscribers ?? 0,
      }));

      const detach = attachSubscriber(key);
      attachedReleaseRef.current = detach;

      try {
        const result = await fetchPromise;
        setData(result ?? null);
        setError(null);
      } catch (err) {
        if (controller.signal.aborted) {
          setError('Request was aborted');
        } else {
          setError(normalizeError(err));
        }
      } finally {
        detach();
        attachedReleaseRef.current = null;
        setLoading(false);
      }
    },
    [memoizedOptions]
  );

  useEffect(() => {
    runQuery();

    return () => {
      if (attachedReleaseRef.current) {
        attachedReleaseRef.current();
        attachedReleaseRef.current = null;
      }
    };
  }, [runQuery]);

  const refresh = useCallback(async () => {
    await runQuery(true);
  }, [runQuery]);

  const revalidate = useCallback(async () => {
    await runQuery(false);
  }, [runQuery]);

  return {
    data,
    loading,
    error,
    refresh,
    revalidate,
  };
}
