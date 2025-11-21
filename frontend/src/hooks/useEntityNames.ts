import { useState, useEffect } from 'react';
import { candidateApi, committeeApi, Contribution } from '../services/api';
import { TopEntity } from '../utils/donorAnalysisUtils';

interface UseEntityNamesResult {
  candidateNames: Record<string, string>;
  committeeNames: Record<string, string>;
  loading: boolean;
}

export function useEntityNames(
  contributions: Contribution[],
  topCandidates: TopEntity[],
  topCommittees: TopEntity[]
): UseEntityNamesResult {
  const [candidateNames, setCandidateNames] = useState<Record<string, string>>({});
  const [committeeNames, setCommitteeNames] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(false);

  // Fetch candidate names for top candidates
  useEffect(() => {
    if (topCandidates.length === 0) return;

    const abortController = new AbortController();

    const fetchCandidateNames = async () => {
      const uniqueCandidateIds = [
        ...new Set(topCandidates.map((c) => c.candidateId).filter((id): id is string => Boolean(id))),
      ];
      if (uniqueCandidateIds.length === 0) return;

      setLoading(true);
      const names: Record<string, string> = {};

      await Promise.all(
        uniqueCandidateIds.map(async (candidateId) => {
          try {
            const candidate = await candidateApi.getById(candidateId, abortController.signal);
            if (!abortController.signal.aborted) {
              names[candidateId] = candidate.name || candidateId;
            }
          } catch (err: any) {
            // If fetch fails or aborted, use the ID as fallback
            if (err.name !== 'AbortError' && !abortController.signal.aborted) {
              names[candidateId] = candidateId;
            }
          }
        })
      );

      if (!abortController.signal.aborted) {
        setCandidateNames((prev) => ({ ...prev, ...names }));
        setLoading(false);
      }
    };

    fetchCandidateNames();

    return () => {
      abortController.abort();
    };
  }, [topCandidates]);

  // Fetch committee names for top committees
  useEffect(() => {
    if (topCommittees.length === 0) return;

    const abortController = new AbortController();

    const fetchCommitteeNames = async () => {
      const uniqueCommitteeIds = [
        ...new Set(topCommittees.map((c) => c.committeeId).filter((id): id is string => Boolean(id))),
      ];
      if (uniqueCommitteeIds.length === 0) return;

      setLoading(true);
      const names: Record<string, string> = {};

      await Promise.all(
        uniqueCommitteeIds.map(async (committeeId) => {
          try {
            const committee = await committeeApi.getById(committeeId, abortController.signal);
            if (!abortController.signal.aborted) {
              names[committeeId] = committee.name || committeeId;
            }
          } catch (err: any) {
            // If fetch fails or aborted, use the ID as fallback
            if (err.name !== 'AbortError' && !abortController.signal.aborted) {
              names[committeeId] = committeeId;
            }
          }
        })
      );

      if (!abortController.signal.aborted) {
        setCommitteeNames((prev) => ({ ...prev, ...names }));
        setLoading(false);
      }
    };

    fetchCommitteeNames();

    return () => {
      abortController.abort();
    };
  }, [topCommittees]);

  // Fetch committee names for all contributions
  useEffect(() => {
    if (contributions.length === 0) return;

    const abortController = new AbortController();

    const fetchAllCommitteeNames = async () => {
      const uniqueCommitteeIds = [
        ...new Set(contributions.map((c) => c.committee_id).filter((id): id is string => Boolean(id))),
      ];

      if (uniqueCommitteeIds.length === 0) return;

      setLoading(true);
      const names: Record<string, string> = {};

      await Promise.all(
        uniqueCommitteeIds.map(async (committeeId) => {
          try {
            const committee = await committeeApi.getById(committeeId, abortController.signal);
            if (!abortController.signal.aborted) {
              names[committeeId] = committee.name || committeeId;
            }
          } catch (err: any) {
            // If fetch fails or aborted, use the ID as fallback
            if (err.name !== 'AbortError' && !abortController.signal.aborted) {
              names[committeeId] = committeeId;
            }
          }
        })
      );

      if (!abortController.signal.aborted) {
        setCommitteeNames((prev) => {
          // Only update with names we don't already have
          const newNames: Record<string, string> = {};
          for (const [id, name] of Object.entries(names)) {
            if (!prev[id]) {
              newNames[id] = name;
            }
          }
          return Object.keys(newNames).length > 0 ? { ...prev, ...newNames } : prev;
        });
        setLoading(false);
      }
    };

    fetchAllCommitteeNames();

    return () => {
      abortController.abort();
    };
  }, [contributions]);

  // Fetch candidate names for all contributions
  useEffect(() => {
    if (contributions.length === 0) return;

    const abortController = new AbortController();

    const fetchAllCandidateNames = async () => {
      const uniqueCandidateIds = [
        ...new Set(contributions.map((c) => c.candidate_id).filter((id): id is string => Boolean(id))),
      ];

      if (uniqueCandidateIds.length === 0) return;

      setLoading(true);
      const names: Record<string, string> = {};

      await Promise.all(
        uniqueCandidateIds.map(async (candidateId) => {
          try {
            const candidate = await candidateApi.getById(candidateId, abortController.signal);
            if (!abortController.signal.aborted) {
              names[candidateId] = candidate.name || candidateId;
            }
          } catch (err: any) {
            // If fetch fails or aborted, use the ID as fallback
            if (err.name !== 'AbortError' && !abortController.signal.aborted) {
              names[candidateId] = candidateId;
            }
          }
        })
      );

      if (!abortController.signal.aborted) {
        setCandidateNames((prev) => {
          // Only update with names we don't already have
          const newNames: Record<string, string> = {};
          for (const [id, name] of Object.entries(names)) {
            if (!prev[id]) {
              newNames[id] = name;
            }
          }
          return Object.keys(newNames).length > 0 ? { ...prev, ...newNames } : prev;
        });
        setLoading(false);
      }
    };

    fetchAllCandidateNames();

    return () => {
      abortController.abort();
    };
  }, [contributions]);

  return {
    candidateNames,
    committeeNames,
    loading,
  };
}

