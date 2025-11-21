import { Contribution, AggregatedDonor } from '../services/api';
import { parseDate, getDateTimestamp } from './dateUtils';

export interface SummaryStats {
  totalAmount: number;
  uniqueCandidates: number;
  uniqueCommittees: number;
  averageContribution: number;
  totalContributions: number;
  uniqueDonors: number;
  nameVariations: number;
}

export interface ChartData {
  labels: string[];
  datasets: Array<{
    label: string;
    data: number[];
    borderColor?: string;
    backgroundColor?: string;
    tension?: number;
  }>;
}

export interface ContributionsByState {
  [state: string]: {
    amount: number;
    count: number;
  };
}

export interface TopEntity {
  amount: number;
  count: number;
  candidateId?: string;
  committeeId?: string;
}

export interface AmountDistribution {
  labels: string[];
  datasets: Array<{
    label: string;
    data: number[];
    backgroundColor: string;
  }>;
}

/**
 * Calculate summary statistics for contributions or aggregated donors
 */
export function calculateSummaryStats(
  contributions: Contribution[],
  aggregatedDonors: AggregatedDonor[],
  viewAggregated: boolean
): SummaryStats {
  if (viewAggregated && aggregatedDonors.length > 0) {
    const totalAmount = aggregatedDonors.reduce((sum, d) => sum + d.total_amount, 0);
    const totalContributions = aggregatedDonors.reduce((sum, d) => sum + d.contribution_count, 0);
    const nameVariations = aggregatedDonors.reduce(
      (sum, d) => sum + (d.all_names.length > 1 ? 1 : 0),
      0
    );
    const averageContribution =
      aggregatedDonors.length > 0 ? totalAmount / aggregatedDonors.length : 0;

    return {
      totalAmount,
      uniqueCandidates: 0, // Not available in aggregated view
      uniqueCommittees: 0, // Not available in aggregated view
      averageContribution,
      totalContributions,
      uniqueDonors: aggregatedDonors.length,
      nameVariations,
    };
  } else {
    const totalAmount = contributions.reduce((sum, c) => sum + c.contribution_amount, 0);
    const uniqueCandidates = new Set(
      contributions.map((c) => c.candidate_id).filter(Boolean)
    ).size;
    const uniqueCommittees = new Set(
      contributions.map((c) => c.committee_id).filter(Boolean)
    ).size;
    const averageContribution =
      contributions.length > 0 ? totalAmount / contributions.length : 0;

    return {
      totalAmount,
      uniqueCandidates,
      uniqueCommittees,
      averageContribution,
      totalContributions: contributions.length,
      uniqueDonors: 0, // Not available in individual view
      nameVariations: 0, // Not available in individual view
    };
  }
}

/**
 * Process contributions data for time series chart
 */
export function processChartData(contributions: Contribution[]): ChartData | null {
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
}

/**
 * Process contributions by state
 */
export function processContributionsByState(
  contributions: Contribution[]
): ContributionsByState {
  const byState: ContributionsByState = {};
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
}

/**
 * Process top candidates from contributions
 */
export function processTopCandidates(contributions: Contribution[]): TopEntity[] {
  const byCandidate: Record<string, TopEntity> = {};
  contributions.forEach((c) => {
    if (c.candidate_id) {
      if (!byCandidate[c.candidate_id]) {
        byCandidate[c.candidate_id] = {
          amount: 0,
          count: 0,
          candidateId: c.candidate_id,
        };
      }
      byCandidate[c.candidate_id].amount += c.contribution_amount;
      byCandidate[c.candidate_id].count += 1;
    }
  });
  return Object.values(byCandidate)
    .sort((a, b) => b.amount - a.amount)
    .slice(0, 10);
}

/**
 * Process top committees from contributions
 */
export function processTopCommittees(contributions: Contribution[]): TopEntity[] {
  const byCommittee: Record<string, TopEntity> = {};
  contributions.forEach((c) => {
    if (c.committee_id) {
      if (!byCommittee[c.committee_id]) {
        byCommittee[c.committee_id] = {
          amount: 0,
          count: 0,
          committeeId: c.committee_id,
        };
      }
      byCommittee[c.committee_id].amount += c.contribution_amount;
      byCommittee[c.committee_id].count += 1;
    }
  });
  return Object.values(byCommittee)
    .sort((a, b) => b.amount - a.amount)
    .slice(0, 10);
}

/**
 * Calculate contribution frequency (contributions per day)
 */
export function processContributionFrequency(contributions: Contribution[]): number | null {
  if (contributions.length === 0) return null;
  const dates = contributions
    .map((c) => parseDate(c.contribution_date))
    .filter((d): d is Date => d !== null);
  if (dates.length === 0) return null;

  const timestamps = dates.map((d) => d.getTime());
  const firstDate = new Date(Math.min(...timestamps));
  const lastDate = new Date(Math.max(...timestamps));
  const daysDiff = Math.ceil(
    (lastDate.getTime() - firstDate.getTime()) / (1000 * 60 * 60 * 24)
  );
  return daysDiff > 0 ? contributions.length / daysDiff : 0;
}

/**
 * Process amount distribution for histogram
 */
export function processAmountDistribution(contributions: Contribution[]): AmountDistribution | null {
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
}

/**
 * Sort contributions by column and direction
 */
export function sortContributions(
  contributions: Contribution[],
  sortColumn: string | null,
  sortDirection: 'asc' | 'desc',
  candidateNames: Record<string, string>,
  committeeNames: Record<string, string>
): Contribution[] {
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
}

/**
 * Sort aggregated donors by column and direction
 */
export function sortAggregatedDonors(
  aggregatedDonors: AggregatedDonor[],
  sortColumn: string | null,
  sortDirection: 'asc' | 'desc'
): AggregatedDonor[] {
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
}

/**
 * Paginate an array of items
 */
export function paginate<T>(items: T[], currentPage: number, itemsPerPage: number): T[] {
  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  return items.slice(startIndex, endIndex);
}

/**
 * Calculate total pages for pagination
 */
export function calculateTotalPages(itemsCount: number, itemsPerPage: number): number {
  return Math.ceil(itemsCount / itemsPerPage);
}

