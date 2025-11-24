# Hooks

## `useCachedQuery`

`useCachedQuery` is a lightweight data-fetching helper that standardizes caching, stale-while-revalidate behavior, and request deduplication for API-driven hooks.

### Defaults
- **Stale time:** 5 minutes. Cached responses served immediately when fresh.
- **Stale-while-revalidate:** Stale cached data is returned instantly while a background refetch keeps results up to date.
- **Request deduplication:** Concurrent requests for the same `queryKey` share a single network call. Aborts are coordinated so a request is only cancelled when no subscribers remain.

### Basic usage
```
const fetchCandidate = useCallback(
  (signal: AbortSignal) => candidateApi.getById(candidateId!, signal),
  [candidateId]
);

const { data, loading, error, refresh } = useCachedQuery<Candidate>({
  queryKey: `candidate:${candidateId}`,
  fetcher: fetchCandidate,
  enabled: Boolean(candidateId),
});
```

### Guidance for new hooks
- Use a **stable `queryKey`** that uniquely describes the request inputs.
- Wrap API calls in `useCallback` so `fetcher` identities stay stable across renders.
- Pass `enabled: false` when the inputs required to make a request are missing.
- Reuse the returned `refresh` method to force a new request when a manual reload is needed (in-flight requests are canceled so
  the latest refresh wins).
- Rely on the hook to handle aborts and set-state guards instead of adding custom `AbortController` logic.
