export type CachePredicate = (key: string, value: unknown) => boolean;

class CacheManager {
  private caches = new Map<string, Map<string, unknown>>();
  private versions = new Map<string, number>();

  private getNamespace(namespace: string): Map<string, unknown> {
    if (!this.caches.has(namespace)) {
      this.caches.set(namespace, new Map());
    }
    return this.caches.get(namespace)!;
  }

  get<T>(namespace: string, key: string): T | undefined {
    return this.getNamespace(namespace).get(key) as T | undefined;
  }

  set<T>(namespace: string, key: string, value: T): void {
    this.getNamespace(namespace).set(key, value);
  }

  clear(namespace: string, predicate?: CachePredicate): void {
    const cache = this.getNamespace(namespace);
    for (const [key, value] of cache.entries()) {
      if (!predicate || predicate(key, value)) {
        cache.delete(key);
      }
    }
    this.bumpVersion(namespace);
  }

  clearByPrefix(namespace: string, prefix: string): void {
    this.clear(namespace, (key) => key.startsWith(prefix));
  }

  bumpVersion(namespace: string): number {
    const current = this.versions.get(namespace) ?? 0;
    const next = current + 1;
    this.versions.set(namespace, next);
    return next;
  }

  getVersion(namespace: string): number {
    return this.versions.get(namespace) ?? 0;
  }
}

export const cacheManager = new CacheManager();

export const CacheNamespaces = {
  financials: 'financials',
  contributions: 'contributions',
  fraudAnalysis: 'fraudAnalysis',
} as const;
