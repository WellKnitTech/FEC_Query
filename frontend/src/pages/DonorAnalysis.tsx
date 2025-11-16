import { useState, useMemo, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { contributionApi, Contribution, exportApi, candidateApi, committeeApi, UniqueContributor, AggregatedDonor } from '../services/api';
import { Line, Bar } from 'react-chartjs-2';
import { parseDate, formatDate, getDateTimestamp } from '../utils/dateUtils';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend
);

export default function DonorAnalysis() {
  const navigate = useNavigate();
  const [contributorName, setContributorName] = useState('');
  const [contributions, setContributions] = useState<Contribution[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [committeeNames, setCommitteeNames] = useState<Record<string, string>>({});
  const [candidateNames, setCandidateNames] = useState<Record<string, string>>({});
  const [uniqueContributors, setUniqueContributors] = useState<UniqueContributor[]>([]);
  const [showUniqueContributors, setShowUniqueContributors] = useState(false);
  const [selectedContributor, setSelectedContributor] = useState<string | null>(null);
  const [minDate, setMinDate] = useState<string>('');
  const [maxDate, setMaxDate] = useState<string>('');
  const [minAmount, setMinAmount] = useState<number | undefined>(undefined);
  const [maxAmount, setMaxAmount] = useState<number | undefined>(undefined);
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc');
  const [currentPage, setCurrentPage] = useState<number>(1);
  const [itemsPerPage, setItemsPerPage] = useState<number>(25);
  const [showEmployer, setShowEmployer] = useState<boolean>(false);
  const [showOccupation, setShowOccupation] = useState<boolean>(false);
  const [viewAggregated, setViewAggregated] = useState<boolean>(false);
  const [aggregatedDonors, setAggregatedDonors] = useState<AggregatedDonor[]>([]);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!contributorName.trim()) return;

    setLoading(true);
    setError(null);
    setSelectedContributor(null);
    setContributions([]);
    setAggregatedDonors([]);
    
    try {
      // First, get unique contributors matching the search term
      const unique = await contributionApi.getUniqueContributors(contributorName.trim(), 100);
      setUniqueContributors(unique);
      
      // If only one match or search term is very specific, show contributions directly
      // Otherwise, show the list of unique contributors first
      if (unique.length === 1) {
        // Only one match, show contributions directly
        if (viewAggregated) {
          const aggregated = await contributionApi.getAggregatedDonors({
            contributor_name: unique[0].name,
            min_date: minDate || undefined,
            max_date: maxDate || undefined,
            min_amount: minAmount,
            max_amount: maxAmount,
            limit: 1000,
          });
          setAggregatedDonors(aggregated);
        } else {
          const data = await contributionApi.get({
            contributor_name: unique[0].name,
            min_date: minDate || undefined,
            max_date: maxDate || undefined,
            min_amount: minAmount,
            max_amount: maxAmount,
            limit: 1000,
          });
          setContributions(data);
        }
        setSelectedContributor(unique[0].name);
        setShowUniqueContributors(false);
      } else if (unique.length > 1) {
        // Multiple matches, show the list first
        setShowUniqueContributors(true);
      } else {
        // No matches, try searching anyway
        if (viewAggregated) {
          const aggregated = await contributionApi.getAggregatedDonors({
            contributor_name: contributorName,
            min_date: minDate || undefined,
            max_date: maxDate || undefined,
            min_amount: minAmount,
            max_amount: maxAmount,
            limit: 1000,
          });
          setAggregatedDonors(aggregated);
        } else {
          const data = await contributionApi.get({
            contributor_name: contributorName,
            min_date: minDate || undefined,
            max_date: maxDate || undefined,
            min_amount: minAmount,
            max_amount: maxAmount,
            limit: 1000,
          });
          setContributions(data);
        }
        setShowUniqueContributors(false);
      }
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to search contributions');
    } finally {
      setLoading(false);
    }
  };

  const handleSelectContributor = async (contributorName: string) => {
    setSelectedContributor(contributorName);
    setShowUniqueContributors(false);
    setLoading(true);
    setError(null);
    
    try {
      if (viewAggregated) {
        const aggregated = await contributionApi.getAggregatedDonors({
          contributor_name: contributorName,
          min_date: minDate || undefined,
          max_date: maxDate || undefined,
          min_amount: minAmount,
          max_amount: maxAmount,
          limit: 1000,
        });
        setAggregatedDonors(aggregated);
      } else {
        const data = await contributionApi.get({
          contributor_name: contributorName,
          min_date: minDate || undefined,
          max_date: maxDate || undefined,
          min_amount: minAmount,
          max_amount: maxAmount,
          limit: 1000,
        });
        setContributions(data);
      }
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to load contributions');
    } finally {
      setLoading(false);
    }
  };

  const handleDatePreset = (preset: 'last30' | 'lastYear' | 'thisCycle' | 'allTime') => {
    const today = new Date();
    const currentYear = today.getFullYear();
    const currentCycle = (currentYear % 2 === 0) ? currentYear : currentYear - 1;
    
    switch (preset) {
      case 'last30':
        const thirtyDaysAgo = new Date(today);
        thirtyDaysAgo.setDate(today.getDate() - 30);
        setMinDate(thirtyDaysAgo.toISOString().split('T')[0]);
        setMaxDate(today.toISOString().split('T')[0]);
        break;
      case 'lastYear':
        const oneYearAgo = new Date(today);
        oneYearAgo.setFullYear(today.getFullYear() - 1);
        setMinDate(oneYearAgo.toISOString().split('T')[0]);
        setMaxDate(today.toISOString().split('T')[0]);
        break;
      case 'thisCycle':
        setMinDate(`${currentCycle}-01-01`);
        setMaxDate(today.toISOString().split('T')[0]);
        break;
      case 'allTime':
        setMinDate('');
        setMaxDate('');
        break;
    }
  };

  const handleAmountPreset = (preset: '0-100' | '100-500' | '500+' | 'all') => {
    switch (preset) {
      case '0-100':
        setMinAmount(0);
        setMaxAmount(100);
        break;
      case '100-500':
        setMinAmount(100);
        setMaxAmount(500);
        break;
      case '500+':
        setMinAmount(500);
        setMaxAmount(undefined);
        break;
      case 'all':
        setMinAmount(undefined);
        setMaxAmount(undefined);
        break;
    }
  };

  const handleClearFilters = () => {
    setContributorName('');
    setMinDate('');
    setMaxDate('');
    setMinAmount(undefined);
    setMaxAmount(undefined);
    setContributions([]);
    setSelectedContributor(null);
    setShowUniqueContributors(false);
    setError(null);
    setSortColumn(null);
    setSortDirection('desc');
    setCurrentPage(1);
  };

  const handleExport = async (format: 'csv' | 'excel') => {
    try {
      // Use selected contributor name if available, otherwise use search term
      const nameToExport = selectedContributor || contributorName;
      await exportApi.exportContributions(format, {
        contributor_name: nameToExport,
        min_date: minDate || undefined,
        max_date: maxDate || undefined,
        min_amount: minAmount,
        max_amount: maxAmount,
      });
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to export contributions');
    }
  };

  // Calculate summary statistics
  const totalAmount = contributions.reduce((sum, c) => sum + c.contribution_amount, 0);
  const uniqueCandidates = new Set(contributions.map((c) => c.candidate_id).filter(Boolean)).size;
  const uniqueCommittees = new Set(contributions.map((c) => c.committee_id).filter(Boolean)).size;
  const averageContribution = contributions.length > 0 ? totalAmount / contributions.length : 0;

  // Process data for visualizations
  const chartData = useMemo(() => {
    if (contributions.length === 0) return null;

    // Group by month
    const byMonth: Record<string, number> = {};
    contributions.forEach((c) => {
      if (c.contribution_date) {
        const date = parseDate(c.contribution_date);
        if (!date) return;
        const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
        byMonth[monthKey] = (byMonth[monthKey] || 0) + c.contribution_amount;
      }
    });

    const sortedMonths = Object.keys(byMonth).sort();
    return {
      labels: sortedMonths,
      datasets: [
        {
          label: 'Contributions by Month',
          data: sortedMonths.map((m) => byMonth[m]),
          borderColor: 'rgb(59, 130, 246)',
          backgroundColor: 'rgba(59, 130, 246, 0.1)',
          tension: 0.1,
        },
      ],
    };
  }, [contributions]);

  // Geographic breakdown
  const contributionsByState = useMemo(() => {
    const byState: Record<string, { amount: number; count: number }> = {};
    contributions.forEach((c) => {
      if (c.contributor_state) {
        const state = c.contributor_state;
        if (!byState[state]) {
          byState[state] = { amount: 0, count: 0 };
        }
        byState[state].amount += c.contribution_amount;
        byState[state].count += 1;
      }
    });
    return byState;
  }, [contributions]);

  // Top candidates
  const topCandidates = useMemo(() => {
    const byCandidate: Record<string, { amount: number; count: number; candidateId: string }> = {};
    contributions.forEach((c) => {
      if (c.candidate_id) {
        if (!byCandidate[c.candidate_id]) {
          byCandidate[c.candidate_id] = { amount: 0, count: 0, candidateId: c.candidate_id };
        }
        byCandidate[c.candidate_id].amount += c.contribution_amount;
        byCandidate[c.candidate_id].count += 1;
      }
    });
    return Object.values(byCandidate)
      .sort((a, b) => b.amount - a.amount)
      .slice(0, 10);
  }, [contributions]);

  // Top committees
  const topCommittees = useMemo(() => {
    const byCommittee: Record<string, { amount: number; count: number; committeeId: string }> = {};
    contributions.forEach((c) => {
      if (c.committee_id) {
        if (!byCommittee[c.committee_id]) {
          byCommittee[c.committee_id] = { amount: 0, count: 0, committeeId: c.committee_id };
        }
        byCommittee[c.committee_id].amount += c.contribution_amount;
        byCommittee[c.committee_id].count += 1;
      }
    });
    return Object.values(byCommittee)
      .sort((a, b) => b.amount - a.amount)
      .slice(0, 10);
  }, [contributions]);

  // Fetch committee names for top committees
  useEffect(() => {
    if (topCommittees.length === 0) return;
    
    const abortController = new AbortController();
    
    const fetchCommitteeNames = async () => {
      const uniqueCommitteeIds = [...new Set(topCommittees.map(c => c.committeeId))];
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
        setCommitteeNames(names);
      }
    };

    fetchCommitteeNames();
    
    return () => {
      abortController.abort();
    };
  }, [topCommittees]);

  // Fetch candidate names for top candidates
  useEffect(() => {
    if (topCandidates.length === 0) return;
    
    const abortController = new AbortController();
    
    const fetchCandidateNames = async () => {
      const uniqueCandidateIds = [...new Set(topCandidates.map(c => c.candidateId))];
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
        setCandidateNames(names);
      }
    };

    fetchCandidateNames();
    
    return () => {
      abortController.abort();
    };
  }, [topCandidates]);

  // Fetch committee names for all contributions
  useEffect(() => {
    if (contributions.length === 0) return;
    
    const abortController = new AbortController();
    
    const fetchAllCommitteeNames = async () => {
      const uniqueCommitteeIds = [...new Set(
        contributions
          .map(c => c.committee_id)
          .filter((id): id is string => Boolean(id))
      )];
      
      // Only fetch committees we don't already have
      const missingIds = uniqueCommitteeIds.filter(id => !committeeNames[id]);
      if (missingIds.length === 0) return;
      
      const names: Record<string, string> = { ...committeeNames };
      
      await Promise.all(
        missingIds.map(async (committeeId) => {
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
        setCommitteeNames(names);
      }
    };

    fetchAllCommitteeNames();
    
    return () => {
      abortController.abort();
    };
  }, [contributions, committeeNames]);

  // Fetch candidate names for all contributions
  useEffect(() => {
    if (contributions.length === 0) return;
    
    const abortController = new AbortController();
    
    const fetchAllCandidateNames = async () => {
      const uniqueCandidateIds = [...new Set(
        contributions
          .map(c => c.candidate_id)
          .filter((id): id is string => Boolean(id))
      )];
      
      // Only fetch candidates we don't already have
      const missingIds = uniqueCandidateIds.filter(id => !candidateNames[id]);
      if (missingIds.length === 0) return;
      
      const names: Record<string, string> = { ...candidateNames };
      
      await Promise.all(
        missingIds.map(async (candidateId) => {
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
        setCandidateNames(names);
      }
    };

    fetchAllCandidateNames();
    
    return () => {
      abortController.abort();
    };
  }, [contributions, candidateNames]);

  // Contribution frequency analysis
  const contributionFrequency = useMemo(() => {
    if (contributions.length === 0) return null;
    const dates = contributions
      .map((c) => parseDate(c.contribution_date))
      .filter((d): d is Date => d !== null);
    if (dates.length === 0) return null;

    const timestamps = dates.map((d) => d.getTime());
    const firstDate = new Date(Math.min(...timestamps));
    const lastDate = new Date(Math.max(...timestamps));
    const daysDiff = Math.ceil((lastDate.getTime() - firstDate.getTime()) / (1000 * 60 * 60 * 24));
    return daysDiff > 0 ? contributions.length / daysDiff : 0;
  }, [contributions]);

  // Amount distribution (histogram data)
  const amountDistribution = useMemo(() => {
    const ranges = [
      { label: '$0-$100', min: 0, max: 100, count: 0 },
      { label: '$100-$500', min: 100, max: 500, count: 0 },
      { label: '$500-$1,000', min: 500, max: 1000, count: 0 },
      { label: '$1,000-$5,000', min: 1000, max: 5000, count: 0 },
      { label: '$5,000+', min: 5000, max: Infinity, count: 0 },
    ];

    contributions.forEach((c) => {
      const amount = c.contribution_amount;
      for (const range of ranges) {
        if (amount >= range.min && amount < range.max) {
          range.count++;
          break;
        }
      }
    });

    return {
      labels: ranges.map((r) => r.label),
      datasets: [
        {
          label: 'Number of Contributions',
          data: ranges.map((r) => r.count),
          backgroundColor: 'rgba(59, 130, 246, 0.6)',
        },
      ],
    };
  }, [contributions]);

  // Sort contributions
  const sortedContributions = useMemo(() => {
    if (!sortColumn) return contributions;
    
    const sorted = [...contributions].sort((a, b) => {
      let aVal: any;
      let bVal: any;
      
      switch (sortColumn) {
        case 'date':
          aVal = getDateTimestamp(a.contribution_date);
          bVal = getDateTimestamp(b.contribution_date);
          break;
        case 'amount':
          aVal = a.contribution_amount || 0;
          bVal = b.contribution_amount || 0;
          break;
        case 'contributor':
          aVal = (a.contributor_name || '').toLowerCase();
          bVal = (b.contributor_name || '').toLowerCase();
          break;
        case 'candidate':
          aVal = (candidateNames[a.candidate_id || ''] || a.candidate_id || '').toLowerCase();
          bVal = (candidateNames[b.candidate_id || ''] || b.candidate_id || '').toLowerCase();
          break;
        case 'committee':
          aVal = (committeeNames[a.committee_id || ''] || a.committee_id || '').toLowerCase();
          bVal = (committeeNames[b.committee_id || ''] || b.committee_id || '').toLowerCase();
          break;
        case 'location':
          aVal = `${a.contributor_city || ''}, ${a.contributor_state || ''}`.toLowerCase();
          bVal = `${b.contributor_city || ''}, ${b.contributor_state || ''}`.toLowerCase();
          break;
        case 'employer':
          aVal = (a.contributor_employer || '').toLowerCase();
          bVal = (b.contributor_employer || '').toLowerCase();
          break;
        case 'occupation':
          aVal = (a.contributor_occupation || '').toLowerCase();
          bVal = (b.contributor_occupation || '').toLowerCase();
          break;
        default:
          return 0;
      }
      
      if (aVal < bVal) return sortDirection === 'asc' ? -1 : 1;
      if (aVal > bVal) return sortDirection === 'asc' ? 1 : -1;
      return 0;
    });
    
    return sorted;
  }, [contributions, sortColumn, sortDirection, candidateNames, committeeNames]);

  // Paginate contributions
  const paginatedContributions = useMemo(() => {
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    return sortedContributions.slice(startIndex, endIndex);
  }, [sortedContributions, currentPage, itemsPerPage]);

  const totalPages = Math.ceil(sortedContributions.length / itemsPerPage);

  // Sort aggregated donors
  const sortedAggregatedDonors = useMemo(() => {
    if (!aggregatedDonors || aggregatedDonors.length === 0) return [];
    if (!sortColumn) return aggregatedDonors;
    
    const sorted = [...aggregatedDonors].sort((a, b) => {
      let aVal: any;
      let bVal: any;
      
      switch (sortColumn) {
        case 'name':
          aVal = (a.canonical_name || '').toLowerCase();
          bVal = (b.canonical_name || '').toLowerCase();
          break;
        case 'amount':
          aVal = a.total_amount || 0;
          bVal = b.total_amount || 0;
          break;
        case 'count':
          aVal = a.contribution_count || 0;
          bVal = b.contribution_count || 0;
          break;
        case 'state':
          aVal = (a.canonical_state || '').toLowerCase();
          bVal = (b.canonical_state || '').toLowerCase();
          break;
        case 'employer':
          aVal = (a.canonical_employer || '').toLowerCase();
          bVal = (b.canonical_employer || '').toLowerCase();
          break;
        case 'confidence':
          aVal = a.match_confidence || 0;
          bVal = b.match_confidence || 0;
          break;
        default:
          return 0;
      }
      
      if (aVal < bVal) return sortDirection === 'asc' ? -1 : 1;
      if (aVal > bVal) return sortDirection === 'asc' ? 1 : -1;
      return 0;
    });
    
    return sorted;
  }, [aggregatedDonors, sortColumn, sortDirection]);

  // Paginate aggregated donors
  const paginatedAggregatedDonors = useMemo(() => {
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    return sortedAggregatedDonors.slice(startIndex, endIndex);
  }, [sortedAggregatedDonors, currentPage, itemsPerPage]);

  const totalPagesAggregated = Math.ceil(sortedAggregatedDonors.length / itemsPerPage);

  const handleSort = (column: string) => {
    if (sortColumn === column) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortColumn(column);
      setSortDirection('desc');
    }
    setCurrentPage(1); // Reset to first page when sorting
  };

  const getSortIcon = (column: string) => {
    if (sortColumn !== column) {
      return <span className="text-gray-400">↕</span>;
    }
    return sortDirection === 'asc' ? <span className="text-blue-600">↑</span> : <span className="text-blue-600">↓</span>;
  };

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <h1 className="text-3xl font-bold text-gray-900 mb-6">Donor Analysis</h1>

      <form onSubmit={handleSearch} className="mb-6">
        <div className="space-y-4">
          <div className="flex gap-2">
            <input
              type="text"
              value={contributorName}
              onChange={(e) => setContributorName(e.target.value)}
              placeholder="Enter contributor name..."
              className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
            <button
              type="submit"
              disabled={loading}
              className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
            >
              {loading ? 'Searching...' : 'Search'}
            </button>
            <button
              type="button"
              onClick={handleClearFilters}
              className="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300"
            >
              Clear Filters
            </button>
            {contributions.length > 0 && (
              <>
                <button
                  type="button"
                  onClick={() => handleExport('csv')}
                  className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700"
                >
                  Export CSV
                </button>
                <button
                  type="button"
                  onClick={() => handleExport('excel')}
                  className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700"
                >
                  Export Excel
                </button>
              </>
            )}
          </div>
          
          {/* Date Range Filters */}
          <div className="flex flex-wrap gap-4 items-end">
            <div className="flex-1 min-w-[200px]">
              <label className="block text-sm font-medium text-gray-700 mb-1">Date Range</label>
              <div className="flex gap-2">
                <input
                  type="date"
                  value={minDate}
                  onChange={(e) => setMinDate(e.target.value)}
                  className="flex-1 px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="Start date"
                />
                <span className="self-center text-gray-500">to</span>
                <input
                  type="date"
                  value={maxDate}
                  onChange={(e) => setMaxDate(e.target.value)}
                  className="flex-1 px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="End date"
                />
              </div>
              <div className="flex gap-2 mt-2">
                <button
                  type="button"
                  onClick={() => handleDatePreset('last30')}
                  className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
                >
                  Last 30 days
                </button>
                <button
                  type="button"
                  onClick={() => handleDatePreset('lastYear')}
                  className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
                >
                  Last year
                </button>
                <button
                  type="button"
                  onClick={() => handleDatePreset('thisCycle')}
                  className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
                >
                  This cycle
                </button>
                <button
                  type="button"
                  onClick={() => handleDatePreset('allTime')}
                  className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
                >
                  All time
                </button>
              </div>
            </div>
            
            {/* Amount Range Filters */}
            <div className="flex-1 min-w-[200px]">
              <label className="block text-sm font-medium text-gray-700 mb-1">Amount Range</label>
              <div className="flex gap-2">
                <input
                  type="number"
                  value={minAmount || ''}
                  onChange={(e) => setMinAmount(e.target.value ? parseFloat(e.target.value) : undefined)}
                  placeholder="Min"
                  min="0"
                  step="0.01"
                  className="flex-1 px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
                <span className="self-center text-gray-500">to</span>
                <input
                  type="number"
                  value={maxAmount || ''}
                  onChange={(e) => setMaxAmount(e.target.value ? parseFloat(e.target.value) : undefined)}
                  placeholder="Max"
                  min="0"
                  step="0.01"
                  className="flex-1 px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
              </div>
              <div className="flex gap-2 mt-2">
                <button
                  type="button"
                  onClick={() => handleAmountPreset('0-100')}
                  className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
                >
                  $0-$100
                </button>
                <button
                  type="button"
                  onClick={() => handleAmountPreset('100-500')}
                  className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
                >
                  $100-$500
                </button>
                <button
                  type="button"
                  onClick={() => handleAmountPreset('500+')}
                  className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
                >
                  $500+
                </button>
                <button
                  type="button"
                  onClick={() => handleAmountPreset('all')}
                  className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
                >
                  All amounts
                </button>
              </div>
            </div>
          </div>
        </div>
      </form>

      {error && (
        <div className="mb-4 p-4 bg-red-100 border border-red-400 text-red-700 rounded">
          {error}
        </div>
      )}

      {showUniqueContributors && uniqueContributors.length > 0 && (
        <div className="bg-white rounded-lg shadow p-6 mb-6">
          <h2 className="text-xl font-semibold mb-4">
            Found {uniqueContributors.length} contributor{uniqueContributors.length !== 1 ? 's' : ''} matching "{contributorName}"
          </h2>
          <p className="text-sm text-gray-600 mb-4">
            Select a contributor to view their contributions:
          </p>
          <div className="space-y-2 max-h-96 overflow-y-auto">
            {uniqueContributors.map((contributor) => (
              <div
                key={contributor.name}
                className="flex justify-between items-center p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer"
                onClick={() => handleSelectContributor(contributor.name)}
              >
                <div>
                  <div className="font-medium text-gray-900">{contributor.name}</div>
                  <div className="text-sm text-gray-600">
                    {contributor.contribution_count} contribution{contributor.contribution_count !== 1 ? 's' : ''}
                  </div>
                </div>
                <div className="text-lg font-bold text-blue-600">
                  ${contributor.total_amount.toLocaleString()}
                </div>
              </div>
            ))}
          </div>
          <button
            onClick={() => {
              setShowUniqueContributors(false);
              handleSelectContributor(contributorName);
            }}
            className="mt-4 px-4 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 text-sm"
          >
            Show all contributions for "{contributorName}"
          </button>
        </div>
      )}

      {selectedContributor && !showUniqueContributors && (
        <div className="mb-4 p-4 bg-blue-50 border border-blue-200 rounded">
          <div className="flex justify-between items-center">
            <span className="text-blue-800">
              Showing contributions for: <strong>{selectedContributor}</strong>
            </span>
            <button
              onClick={() => {
                setSelectedContributor(null);
                setContributions([]);
                setShowUniqueContributors(true);
              }}
              className="text-blue-600 hover:text-blue-800 text-sm underline"
            >
              Change contributor
            </button>
          </div>
        </div>
      )}

      {((viewAggregated && aggregatedDonors.length > 0) || (!viewAggregated && contributions.length > 0)) && (
        <div className="space-y-6">
          {/* View Toggle */}
          <div className="bg-white rounded-lg shadow p-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-4">
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={viewAggregated}
                    onChange={async (e) => {
                      const newView = e.target.checked;
                      setViewAggregated(newView);
                      // Refetch data when toggling view
                      if (selectedContributor || contributorName) {
                        setLoading(true);
                        try {
                          const searchName = selectedContributor || contributorName;
                          if (newView) {
                            const aggregated = await contributionApi.getAggregatedDonors({
                              contributor_name: searchName,
                              min_date: minDate || undefined,
                              max_date: maxDate || undefined,
                              min_amount: minAmount,
                              max_amount: maxAmount,
                              limit: 1000,
                            });
                            setAggregatedDonors(aggregated);
                          } else {
                            const data = await contributionApi.get({
                              contributor_name: searchName,
                              min_date: minDate || undefined,
                              max_date: maxDate || undefined,
                              min_amount: minAmount,
                              max_amount: maxAmount,
                              limit: 1000,
                            });
                            setContributions(data);
                          }
                        } catch (err: any) {
                          setError(err?.response?.data?.detail || err?.message || 'Failed to switch view');
                        } finally {
                          setLoading(false);
                        }
                      }
                    }}
                    className="rounded"
                  />
                  <span className="text-sm font-medium">View Aggregated Donors</span>
                </label>
                <span className="text-xs text-gray-500">
                  {viewAggregated 
                    ? 'Showing unique donors (grouped by name variations)'
                    : 'Showing individual contributions'}
                </span>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-xl font-semibold mb-4">Summary</h2>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <div>
                <div className="text-sm text-gray-600">Total Contributions</div>
                <div className="text-2xl font-bold">
                  ${viewAggregated 
                    ? aggregatedDonors.reduce((sum, d) => sum + d.total_amount, 0).toLocaleString()
                    : totalAmount.toLocaleString()}
                </div>
              </div>
              <div>
                <div className="text-sm text-gray-600">
                  {viewAggregated ? 'Unique Donors' : 'Number of Contributions'}
                </div>
                <div className="text-2xl font-bold">
                  {viewAggregated ? aggregatedDonors.length : contributions.length}
                </div>
              </div>
              {viewAggregated && (
                <div>
                  <div className="text-sm text-gray-600">Name Variations Found</div>
                  <div className="text-2xl font-bold">
                    {aggregatedDonors.reduce((sum, d) => sum + (d.all_names.length > 1 ? 1 : 0), 0)}
                  </div>
                </div>
              )}
              {!viewAggregated && (
                <div>
                  <div className="text-sm text-gray-600">Candidates Supported</div>
                  <div className="text-2xl font-bold">{uniqueCandidates}</div>
                </div>
              )}
              <div>
                <div className="text-sm text-gray-600">Average Contribution</div>
                <div className="text-2xl font-bold">
                  ${viewAggregated
                    ? (aggregatedDonors.length > 0 
                        ? (aggregatedDonors.reduce((sum, d) => sum + d.total_amount, 0) / aggregatedDonors.length).toFixed(2)
                        : '0.00')
                    : averageContribution.toFixed(2)}
                </div>
              </div>
            </div>
          </div>

          {/* Time Series Chart - Only show for individual contributions */}
          {!viewAggregated && chartData && (
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-4">Contributions Over Time</h2>
              <div className="h-64">
                <Line
                  data={chartData}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                      legend: { display: true },
                      title: { display: false },
                    },
                    scales: {
                      y: {
                        beginAtZero: true,
                        ticks: {
                          callback: function (value) {
                            return '$' + value.toLocaleString();
                          },
                        },
                      },
                    },
                  }}
                />
              </div>
            </div>
          )}

          {/* Employer Breakdown */}
          {contributions.some(c => c.contributor_employer) && (
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-4">Top Employers</h2>
              <div className="space-y-2">
                {Object.entries(
                  contributions.reduce((acc, c) => {
                    if (c.contributor_employer) {
                      const employer = c.contributor_employer;
                      if (!acc[employer]) {
                        acc[employer] = { amount: 0, count: 0 };
                      }
                      acc[employer].amount += c.contribution_amount || 0;
                      acc[employer].count += 1;
                    }
                    return acc;
                  }, {} as Record<string, { amount: number; count: number }>)
                )
                  .map(([employer, data]) => ({ employer, ...data }))
                  .sort((a, b) => b.amount - a.amount)
                  .slice(0, 10)
                  .map((item, idx) => (
                    <div key={item.employer} className="flex justify-between items-center p-3 border border-gray-200 rounded-lg">
                      <div>
                        <div className="font-medium">#{idx + 1} {item.employer}</div>
                        <div className="text-sm text-gray-600">{item.count} contribution{item.count !== 1 ? 's' : ''}</div>
                      </div>
                      <div className="text-lg font-bold text-blue-600">
                        ${item.amount.toLocaleString()}
                      </div>
                    </div>
                  ))}
              </div>
            </div>
          )}

          {/* Geographic Breakdown - Enhanced with Bar Chart */}
          {Object.keys(contributionsByState).length > 0 && (
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-4">Contributions by State</h2>
              <div className="mb-4">
                <Bar
                  data={{
                    labels: Object.entries(contributionsByState)
                      .sort((a, b) => b[1].amount - a[1].amount)
                      .slice(0, 15)
                      .map(([state]) => state),
                    datasets: [
                      {
                        label: 'Total Contributions',
                        data: Object.entries(contributionsByState)
                          .sort((a, b) => b[1].amount - a[1].amount)
                          .slice(0, 15)
                          .map(([, data]) => data.amount),
                        backgroundColor: 'rgba(59, 130, 246, 0.6)',
                      },
                    ],
                  }}
                  options={{
                    responsive: true,
                    plugins: {
                      legend: { display: false },
                      tooltip: {
                        callbacks: {
                          label: (context) => {
                            const state = context.label;
                            const data = contributionsByState[state];
                            return [
                              `State: ${state}`,
                              `Amount: $${data.amount.toLocaleString()}`,
                              `Count: ${data.count} contributions`,
                              `Percentage: ${((data.amount / totalAmount) * 100).toFixed(1)}%`,
                            ];
                          },
                        },
                      },
                    },
                    scales: {
                      y: {
                        beginAtZero: true,
                        ticks: {
                          callback: function (value) {
                            return '$' + value.toLocaleString();
                          },
                        },
                      },
                    },
                  }}
                />
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {Object.entries(contributionsByState)
                  .sort((a, b) => b[1].amount - a[1].amount)
                  .slice(0, 12)
                  .map(([state, data]) => (
                    <div 
                      key={state} 
                      className="p-4 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer"
                      onClick={() => {
                        // Filter by state - could add state filter in future
                        // State clicked - handle state selection if needed
                      }}
                    >
                      <div className="font-semibold text-gray-900">{state}</div>
                      <div className="text-2xl font-bold text-blue-600 mt-1">
                        ${data.amount.toLocaleString()}
                      </div>
                      <div className="text-sm text-gray-600 mt-1">
                        {data.count} contribution{data.count !== 1 ? 's' : ''}
                      </div>
                      <div className="text-xs text-gray-500 mt-1">
                        {((data.amount / totalAmount) * 100).toFixed(1)}% of total
                      </div>
                    </div>
                  ))}
              </div>
            </div>
          )}

          {/* Top Candidates and Committees */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {topCandidates.length > 0 && (
              <div className="bg-white rounded-lg shadow p-6">
                <h2 className="text-xl font-semibold mb-4">Top Candidates Supported</h2>
                <div className="space-y-2">
                  {topCandidates.map((candidate, idx) => (
                    <div
                      key={candidate.candidateId}
                      className="flex justify-between items-center p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer"
                      onClick={() => navigate(`/candidate/${candidate.candidateId}`)}
                    >
                      <div>
                        <div className="font-medium">#{idx + 1} {candidateNames[candidate.candidateId] || candidate.candidateId}</div>
                        <div className="text-sm text-gray-600">{candidate.count} contributions</div>
                        {candidateNames[candidate.candidateId] && (
                          <div className="text-xs text-gray-500 mt-1">ID: {candidate.candidateId}</div>
                        )}
                      </div>
                      <div className="text-lg font-bold text-blue-600">
                        ${candidate.amount.toLocaleString()}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {topCommittees.length > 0 && (
              <div className="bg-white rounded-lg shadow p-6">
                <h2 className="text-xl font-semibold mb-4">Top Committees Supported</h2>
                <div className="space-y-2">
                  {topCommittees.map((committee, idx) => (
                    <div
                      key={committee.committeeId}
                      className="flex justify-between items-center p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer"
                      onClick={() => navigate(`/committee/${committee.committeeId}`)}
                    >
                      <div>
                        <div className="font-medium">#{idx + 1} {committeeNames[committee.committeeId] || committee.committeeId}</div>
                        <div className="text-sm text-gray-600">{committee.count} contributions</div>
                        {committeeNames[committee.committeeId] && (
                          <div className="text-xs text-gray-500 mt-1">ID: {committee.committeeId}</div>
                        )}
                      </div>
                      <div className="text-lg font-bold text-blue-600">
                        ${committee.amount.toLocaleString()}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Contribution Velocity - Only show for individual contributions */}
          {!viewAggregated && contributions.length > 0 && (
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-4">Contribution Velocity</h2>
              <div className="h-64">
                <Line
                  data={{
                    labels: Object.keys(
                      contributions.reduce((acc, c) => {
                        if (c.contribution_date) {
                          const date = c.contribution_date.split('T')[0];
                          if (!acc[date]) acc[date] = 0;
                          acc[date] += c.contribution_amount || 0;
                        }
                        return acc;
                      }, {} as Record<string, number>)
                    ).sort(),
                    datasets: [
                      {
                        label: 'Daily Contributions',
                        data: Object.entries(
                          contributions.reduce((acc, c) => {
                            if (c.contribution_date) {
                              const date = c.contribution_date.split('T')[0];
                              if (!acc[date]) acc[date] = 0;
                              acc[date] += c.contribution_amount || 0;
                            }
                            return acc;
                          }, {} as Record<string, number>)
                        )
                          .sort((a, b) => a[0].localeCompare(b[0]))
                          .map(([, amount]) => amount),
                        borderColor: 'rgb(16, 185, 129)',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        tension: 0.1,
                      },
                    ],
                  }}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                      legend: { display: true },
                      tooltip: {
                        callbacks: {
                          label: (context) => `$${(context.parsed.y || 0).toLocaleString()}`,
                        },
                      },
                    },
                    scales: {
                      y: {
                        beginAtZero: true,
                        ticks: {
                          callback: function (value) {
                            return '$' + value.toLocaleString();
                          },
                        },
                      },
                    },
                  }}
                />
              </div>
            </div>
          )}

          {/* Contribution Pattern Analysis */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-xl font-semibold mb-4">Contribution Patterns</h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <div>
                <div className="text-sm text-gray-600 mb-2">Contribution Frequency</div>
                <div className="text-2xl font-bold">
                  {contributionFrequency ? contributionFrequency.toFixed(2) : '0'} per day
                </div>
                <div className="text-xs text-gray-500 mt-1">
                  Average contributions per day over period
                </div>
              </div>
              <div>
                <div className="text-sm text-gray-600 mb-2">Average Contribution Size</div>
                <div className="text-2xl font-bold">${averageContribution.toFixed(2)}</div>
                <div className="text-xs text-gray-500 mt-1">Mean contribution amount</div>
              </div>
              <div>
                <div className="text-sm text-gray-600 mb-2">Committees Supported</div>
                <div className="text-2xl font-bold">{uniqueCommittees}</div>
                <div className="text-xs text-gray-500 mt-1">Unique committees</div>
              </div>
            </div>
            {amountDistribution && (
              <div className="mt-6">
                <h3 className="text-lg font-semibold mb-4">Contribution Amount Distribution</h3>
                <div className="h-48">
                  <Bar
                    data={amountDistribution}
                    options={{
                      responsive: true,
                      maintainAspectRatio: false,
                      plugins: {
                        legend: { display: false },
                      },
                      scales: {
                        y: {
                          beginAtZero: true,
                        },
                      },
                    }}
                  />
                </div>
              </div>
            )}
          </div>

          {/* Aggregated Donors Table */}
          {viewAggregated && aggregatedDonors.length > 0 && (
            <div className="bg-white rounded-lg shadow">
              <div className="px-6 py-4 border-b border-gray-200 flex justify-between items-center">
                <h2 className="text-lg font-semibold">Aggregated Donors ({sortedAggregatedDonors.length})</h2>
                <div className="flex items-center gap-2">
                  <label className="text-sm text-gray-600">Show:</label>
                  <select
                    value={itemsPerPage}
                    onChange={(e) => {
                      setItemsPerPage(Number(e.target.value));
                      setCurrentPage(1);
                    }}
                    className="px-3 py-1 border border-gray-300 rounded-lg text-sm"
                  >
                    <option value={10}>10</option>
                    <option value={25}>25</option>
                    <option value={50}>50</option>
                    <option value={100}>100</option>
                  </select>
                </div>
              </div>
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th 
                        className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                        onClick={() => handleSort('name')}
                      >
                        <div className="flex items-center gap-1">
                          Donor Name
                          {getSortIcon('name')}
                        </div>
                      </th>
                      <th 
                        className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                        onClick={() => handleSort('amount')}
                      >
                        <div className="flex items-center gap-1">
                          Total Amount
                          {getSortIcon('amount')}
                        </div>
                      </th>
                      <th 
                        className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                        onClick={() => handleSort('count')}
                      >
                        <div className="flex items-center gap-1">
                          Contributions
                          {getSortIcon('count')}
                        </div>
                      </th>
                      <th 
                        className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                        onClick={() => handleSort('state')}
                      >
                        <div className="flex items-center gap-1">
                          State
                          {getSortIcon('state')}
                        </div>
                      </th>
                      <th 
                        className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                        onClick={() => handleSort('employer')}
                      >
                        <div className="flex items-center gap-1">
                          Employer
                          {getSortIcon('employer')}
                        </div>
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Date Range
                      </th>
                      <th 
                        className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                        onClick={() => handleSort('confidence')}
                      >
                        <div className="flex items-center gap-1">
                          Match Confidence
                          {getSortIcon('confidence')}
                        </div>
                      </th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {paginatedAggregatedDonors.map((donor, idx) => (
                      <tr key={donor.donor_key || idx} className="hover:bg-gray-50">
                        <td className="px-6 py-4 whitespace-nowrap text-sm">
                          <div className="font-medium text-gray-900">{donor.canonical_name}</div>
                          {donor.all_names.length > 1 && (
                            <div className="text-xs text-gray-500 mt-1" title={donor.all_names.join(', ')}>
                              {donor.all_names.length} name variation{donor.all_names.length !== 1 ? 's' : ''}
                              {donor.all_names.length <= 3 && (
                                <span className="ml-1">({donor.all_names.slice(0, 3).join(', ')})</span>
                              )}
                            </div>
                          )}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                          ${donor.total_amount.toLocaleString()}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {donor.contribution_count}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {donor.canonical_state || 'N/A'}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {donor.canonical_employer || 'N/A'}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {donor.first_contribution_date && donor.last_contribution_date ? (
                            <div>
                              <div>{formatDate(donor.first_contribution_date)}</div>
                              <div className="text-xs text-gray-400">to</div>
                              <div>{formatDate(donor.last_contribution_date)}</div>
                            </div>
                          ) : 'N/A'}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          <div className="flex items-center gap-2">
                            <span>{(donor.match_confidence * 100).toFixed(0)}%</span>
                            <div className="w-16 h-2 bg-gray-200 rounded-full overflow-hidden">
                              <div 
                                className={`h-full ${
                                  donor.match_confidence >= 0.8 ? 'bg-green-500' :
                                  donor.match_confidence >= 0.6 ? 'bg-yellow-500' : 'bg-red-500'
                                }`}
                                style={{ width: `${donor.match_confidence * 100}%` }}
                              />
                            </div>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {/* Pagination Controls */}
              {totalPagesAggregated > 1 && (
                <div className="px-6 py-4 border-t border-gray-200 flex items-center justify-between">
                  <div className="text-sm text-gray-600">
                    Showing {(currentPage - 1) * itemsPerPage + 1} to {Math.min(currentPage * itemsPerPage, sortedAggregatedDonors.length)} of {sortedAggregatedDonors.length} donors
                  </div>
                  <div className="flex gap-2">
                    <button
                      onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                      disabled={currentPage === 1}
                      className="px-3 py-1 border border-gray-300 rounded-lg text-sm hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      Previous
                    </button>
                    <div className="flex gap-1">
                      {Array.from({ length: Math.min(5, totalPagesAggregated) }, (_, i) => {
                        let pageNum: number;
                        if (totalPagesAggregated <= 5) {
                          pageNum = i + 1;
                        } else if (currentPage <= 3) {
                          pageNum = i + 1;
                        } else if (currentPage >= totalPagesAggregated - 2) {
                          pageNum = totalPagesAggregated - 4 + i;
                        } else {
                          pageNum = currentPage - 2 + i;
                        }
                        return (
                          <button
                            key={pageNum}
                            onClick={() => setCurrentPage(pageNum)}
                            className={`px-3 py-1 border rounded-lg text-sm ${
                              currentPage === pageNum
                                ? 'bg-blue-600 text-white border-blue-600'
                                : 'border-gray-300 hover:bg-gray-50'
                            }`}
                          >
                            {pageNum}
                          </button>
                        );
                      })}
                    </div>
                    <button
                      onClick={() => setCurrentPage(prev => Math.min(totalPagesAggregated, prev + 1))}
                      disabled={currentPage === totalPagesAggregated}
                      className="px-3 py-1 border border-gray-300 rounded-lg text-sm hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      Next
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Individual Contributions Table */}
          {!viewAggregated && contributions.length > 0 && (
            <div className="bg-white rounded-lg shadow">
            <div className="px-6 py-4 border-b border-gray-200 flex justify-between items-center">
              <h2 className="text-lg font-semibold">Contributions ({sortedContributions.length})</h2>
              <div className="flex gap-4 items-center">
                <div className="flex items-center gap-2">
                  <label className="text-sm text-gray-600">Show:</label>
                  <select
                    value={itemsPerPage}
                    onChange={(e) => {
                      setItemsPerPage(Number(e.target.value));
                      setCurrentPage(1);
                    }}
                    className="px-3 py-1 border border-gray-300 rounded-lg text-sm"
                  >
                    <option value={10}>10</option>
                    <option value={25}>25</option>
                    <option value={50}>50</option>
                    <option value={100}>100</option>
                  </select>
                </div>
                <div className="flex items-center gap-2">
                  <label className="text-sm text-gray-600">Columns:</label>
                  <label className="flex items-center gap-1 text-sm">
                    <input
                      type="checkbox"
                      checked={showEmployer}
                      onChange={(e) => setShowEmployer(e.target.checked)}
                      className="rounded"
                    />
                    Employer
                  </label>
                  <label className="flex items-center gap-1 text-sm">
                    <input
                      type="checkbox"
                      checked={showOccupation}
                      onChange={(e) => setShowOccupation(e.target.checked)}
                      className="rounded"
                    />
                    Occupation
                  </label>
                </div>
              </div>
            </div>
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th 
                      className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                      onClick={() => handleSort('date')}
                    >
                      <div className="flex items-center gap-1">
                        Date
                        {getSortIcon('date')}
                      </div>
                    </th>
                    <th 
                      className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                      onClick={() => handleSort('amount')}
                    >
                      <div className="flex items-center gap-1">
                        Amount
                        {getSortIcon('amount')}
                      </div>
                    </th>
                    <th 
                      className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                      onClick={() => handleSort('contributor')}
                    >
                      <div className="flex items-center gap-1">
                        Contributor
                        {getSortIcon('contributor')}
                      </div>
                    </th>
                    <th 
                      className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                      onClick={() => handleSort('candidate')}
                    >
                      <div className="flex items-center gap-1">
                        Candidate
                        {getSortIcon('candidate')}
                      </div>
                    </th>
                    <th 
                      className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                      onClick={() => handleSort('committee')}
                    >
                      <div className="flex items-center gap-1">
                        Committee
                        {getSortIcon('committee')}
                      </div>
                    </th>
                    {showEmployer && (
                      <th 
                        className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                        onClick={() => handleSort('employer')}
                      >
                        <div className="flex items-center gap-1">
                          Employer
                          {getSortIcon('employer')}
                        </div>
                      </th>
                    )}
                    {showOccupation && (
                      <th 
                        className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                        onClick={() => handleSort('occupation')}
                      >
                        <div className="flex items-center gap-1">
                          Occupation
                          {getSortIcon('occupation')}
                        </div>
                      </th>
                    )}
                    <th 
                      className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                      onClick={() => handleSort('location')}
                    >
                      <div className="flex items-center gap-1">
                        Location
                        {getSortIcon('location')}
                      </div>
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {paginatedContributions.map((contrib, idx) => (
                    <tr key={idx} className="hover:bg-gray-50">
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {contrib.contribution_date || 'N/A'}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                        ${(contrib.contribution_amount || 0).toLocaleString()}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {contrib.contributor_name || 'N/A'}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {contrib.candidate_id ? (
                          <span 
                            className="text-blue-600 hover:text-blue-800 cursor-pointer"
                            onClick={() => navigate(`/candidate/${contrib.candidate_id}`)}
                          >
                            {candidateNames[contrib.candidate_id] || contrib.candidate_id}
                          </span>
                        ) : (
                          'N/A'
                        )}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {contrib.committee_id ? (
                          <span 
                            className="text-blue-600 hover:text-blue-800 cursor-pointer"
                            onClick={() => navigate(`/committee/${contrib.committee_id}`)}
                          >
                            {committeeNames[contrib.committee_id] || contrib.committee_id}
                          </span>
                        ) : (
                          'N/A'
                        )}
                      </td>
                      {showEmployer && (
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {contrib.contributor_employer || 'N/A'}
                        </td>
                      )}
                      {showOccupation && (
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {contrib.contributor_occupation || 'N/A'}
                        </td>
                      )}
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {contrib.contributor_city && contrib.contributor_state
                          ? `${contrib.contributor_city}, ${contrib.contributor_state}`
                          : 'N/A'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {/* Pagination Controls */}
            {totalPages > 1 && (
              <div className="px-6 py-4 border-t border-gray-200 flex items-center justify-between">
                <div className="text-sm text-gray-600">
                  Showing {(currentPage - 1) * itemsPerPage + 1} to {Math.min(currentPage * itemsPerPage, sortedContributions.length)} of {sortedContributions.length} contributions
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                    disabled={currentPage === 1}
                    className="px-3 py-1 border border-gray-300 rounded-lg text-sm hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    Previous
                  </button>
                  <div className="flex gap-1">
                    {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                      let pageNum: number;
                      if (totalPages <= 5) {
                        pageNum = i + 1;
                      } else if (currentPage <= 3) {
                        pageNum = i + 1;
                      } else if (currentPage >= totalPages - 2) {
                        pageNum = totalPages - 4 + i;
                      } else {
                        pageNum = currentPage - 2 + i;
                      }
                      return (
                        <button
                          key={pageNum}
                          onClick={() => setCurrentPage(pageNum)}
                          className={`px-3 py-1 border rounded-lg text-sm ${
                            currentPage === pageNum
                              ? 'bg-blue-600 text-white border-blue-600'
                              : 'border-gray-300 hover:bg-gray-50'
                          }`}
                        >
                          {pageNum}
                        </button>
                      );
                    })}
                  </div>
                  <button
                    onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))}
                    disabled={currentPage === totalPages}
                    className="px-3 py-1 border border-gray-300 rounded-lg text-sm hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    Next
                  </button>
                </div>
              </div>
            )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

