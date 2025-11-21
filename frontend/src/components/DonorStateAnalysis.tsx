import { useEffect, useState } from 'react';
import { analysisApi, exportApi, DonorStateAnalysis as DonorStateAnalysisType, Candidate, Contribution, AggregatedDonor } from '../services/api';
import { formatDate } from '../utils/dateUtils';
import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
);

interface DonorStateAnalysisProps {
  candidateId?: string;
  candidate?: Candidate;
  cycle?: number;
}


export default function DonorStateAnalysis({
  candidateId,
  candidate,
  cycle,
}: DonorStateAnalysisProps) {
  const [analysis, setAnalysis] = useState<DonorStateAnalysisType | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [outOfStateContributions, setOutOfStateContributions] = useState<Contribution[]>([]);
  const [aggregatedDonors, setAggregatedDonors] = useState<AggregatedDonor[]>([]);
  const [loadingContributions, setLoadingContributions] = useState(false);
  const [contributionsError, setContributionsError] = useState<string | null>(null);
  const [showContributions, setShowContributions] = useState(false);
  const [aggregateMode, setAggregateMode] = useState(false);
  const [exporting, setExporting] = useState(false);

  useEffect(() => {
    if (!candidateId) {
      setLoading(false);
      return;
    }

    // Only show for Senate (S) and House (H) candidates
    if (candidate?.office && candidate.office.toUpperCase() === 'P') {
      setLoading(false);
      return;
    }

    const abortController = new AbortController();
    
    const fetchAnalysis = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await analysisApi.getDonorStates({
          candidate_id: candidateId,
          cycle: cycle,
        }, abortController.signal);
        if (!abortController.signal.aborted) {
          setAnalysis(data);
        }
      } catch (err: any) {
        // Don't set error if request was aborted
        if (err.name === 'AbortError' || abortController.signal.aborted) {
          return;
        }
        const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to load donor state analysis';
        if (!abortController.signal.aborted) {
          setError(errorMessage);
        }
      } finally {
        if (!abortController.signal.aborted) {
          setLoading(false);
        }
      }
    };

    fetchAnalysis();
    
    return () => {
      abortController.abort();
    };
  }, [candidateId, candidate?.office, cycle]);

  const loadOutOfStateContributions = async (aggregate: boolean = false) => {
    if (!candidateId) return;
    
    setLoadingContributions(true);
    setContributionsError(null);
    setAggregateMode(aggregate);
    try {
      const data = await analysisApi.getOutOfStateContributions({
        candidate_id: candidateId,
        cycle: cycle,
        limit: aggregate ? 1000 : 10000,
        aggregate: aggregate,
      });
      
      if (aggregate) {
        setAggregatedDonors(data as AggregatedDonor[]);
        setOutOfStateContributions([]);
      } else {
        setOutOfStateContributions(data as Contribution[]);
        setAggregatedDonors([]);
      }
      setShowContributions(true);
    } catch (err: any) {
      const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to load out-of-state contributions';
      setContributionsError(errorMessage);
    } finally {
      setLoadingContributions(false);
    }
  };

  const handleExport = async () => {
    if (!candidateId) return;
    
    setExporting(true);
    try {
      if (aggregateMode) {
        await exportApi.exportOutOfStateDonors(candidateId, {
          cycle: cycle,
        });
      } else {
        await exportApi.exportOutOfStateContributions(candidateId, {
          cycle: cycle,
        });
      }
    } catch (err: any) {
      alert('Failed to export. Please try again.');
    } finally {
      setExporting(false);
    }
  };

  // Don't render for Presidential candidates
  if (candidate?.office && candidate.office.toUpperCase() === 'P') {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-2xl font-bold text-gray-900 mb-2">Donor State Analysis</h2>
        <p className="text-gray-600">
          Donor state analysis is not applicable to Presidential candidates. This analysis is only available for Senate and House candidates.
        </p>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="animate-pulse">
          <div className="h-6 bg-gray-200 rounded w-1/3 mb-4"></div>
          <div className="h-64 bg-gray-200 rounded"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-2xl font-bold text-gray-900 mb-4">Donor State Analysis</h2>
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <p className="text-red-800">{error}</p>
        </div>
      </div>
    );
  }

  if (!analysis) {
    return null;
  }

  // Prepare chart data - top 15 states by donor count
  const stateEntries = Object.entries(analysis.donors_by_state)
    .sort(([, a], [, b]) => b - a)
    .slice(0, 15);

  const stateLabels = stateEntries.map(([state]) => state);
  const donorCounts = stateEntries.map(([, count]) => count);
  const amounts = stateEntries.map(([state]) => analysis.amounts_by_state[state] || 0);

  const chartData = {
    labels: stateLabels,
    datasets: [
      {
        label: 'Donor Count',
        data: donorCounts,
        backgroundColor: 'rgba(59, 130, 246, 0.6)',
        borderColor: 'rgba(59, 130, 246, 1)',
        borderWidth: 1,
      },
    ],
  };

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: true,
    plugins: {
      legend: {
        display: true,
      },
      title: {
        display: true,
        text: 'Top States by Donor Count',
      },
    },
    scales: {
      y: {
        beginAtZero: true,
      },
    },
  };

  // Sort all states for the table
  const allStateEntries = Object.entries(analysis.donors_by_state)
    .map(([state, count]) => ({
      state,
      donorCount: count,
      donorPercentage: analysis.donor_percentages_by_state[state] || 0,
      amount: analysis.amounts_by_state[state] || 0,
      amountPercentage: analysis.amount_percentages_by_state[state] || 0,
    }))
    .sort((a, b) => b.donorCount - a.donorCount);

  const isInState = (state: string) => {
    return state === analysis.candidate_state;
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-2xl font-bold text-gray-900 mb-2">Donor State Analysis</h2>
        <p className="text-gray-600">
          Analysis of individual donors by state to identify funding sources
        </p>
        <div className="mt-2 text-xs text-gray-500">
          Note: This analysis is based on bulk-imported data. Check Contribution Analysis section for data completeness percentage.
        </div>
      </div>

      {/* Alert Banner for High Out-of-State Funding */}
      {analysis.is_highly_out_of_state && (
        <div className="bg-orange-50 border-l-4 border-orange-500 rounded-lg p-6 shadow">
          <div className="flex items-start">
            <div className="flex-shrink-0">
              <svg className="h-6 w-6 text-orange-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
              </svg>
            </div>
            <div className="ml-3 flex-1">
              <h3 className="text-lg font-semibold text-orange-800">
                High Out-of-State Funding Detected
              </h3>
              <p className="mt-2 text-sm text-orange-700">
                This candidate is receiving more than 50% of their contributions from outside their state.
                {analysis.candidate_state && (
                  <span className="font-medium"> Candidate's state: {analysis.candidate_state}</span>
                )}
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg shadow p-6">
          <div className="text-sm text-gray-600 mb-1">In-State Donors</div>
          <div className="text-3xl font-bold text-green-600">
            {analysis.in_state_donor_percentage.toFixed(1)}%
          </div>
          <div className="text-xs text-gray-500 mt-1">
            {analysis.candidate_state ? `of ${analysis.total_unique_donors.toLocaleString()} donors from ${analysis.candidate_state}` : 'N/A'}
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <div className="text-sm text-gray-600 mb-1">Out-of-State Donors</div>
          <div className={`text-3xl font-bold ${analysis.is_highly_out_of_state ? 'text-orange-600' : 'text-gray-700'}`}>
            {analysis.out_of_state_donor_percentage.toFixed(1)}%
          </div>
          <div className="text-xs text-gray-500 mt-1">
            {analysis.candidate_state ? `of ${analysis.total_unique_donors.toLocaleString()} donors from other states` : 'N/A'}
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <div className="text-sm text-gray-600 mb-1">In-State Amount</div>
          <div className="text-3xl font-bold text-green-600">
            {analysis.in_state_amount_percentage.toFixed(1)}%
          </div>
          <div className="text-xs text-gray-500 mt-1">
            {analysis.candidate_state ? `of $${(analysis.total_contributions / 1000).toFixed(1)}K from ${analysis.candidate_state}` : 'N/A'}
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <div className="text-sm text-gray-600 mb-1">Out-of-State Amount</div>
          <div className={`text-3xl font-bold ${analysis.is_highly_out_of_state ? 'text-orange-600' : 'text-gray-700'}`}>
            {analysis.out_of_state_amount_percentage.toFixed(1)}%
          </div>
          <div className="text-xs text-gray-500 mt-1">
            {analysis.candidate_state ? `of $${(analysis.total_contributions / 1000).toFixed(1)}K from other states` : 'N/A'}
          </div>
        </div>
      </div>

      {/* Chart */}
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold mb-4">Top States by Donor Count</h3>
        <div className="h-96">
          <Bar data={chartData} options={chartOptions} />
        </div>
      </div>

      {/* State Table */}
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold mb-4">Complete State Breakdown</h3>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  State
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Donor Count
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Donor %
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Amount
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Amount %
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {allStateEntries.map((entry) => (
                <tr
                  key={entry.state}
                  className={isInState(entry.state) ? 'bg-green-50' : ''}
                >
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      <span className="text-sm font-medium text-gray-900">{entry.state}</span>
                      {isInState(entry.state) && (
                        <span className="ml-2 inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                          Candidate's State
                        </span>
                      )}
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {entry.donorCount.toLocaleString()}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {entry.donorPercentage.toFixed(2)}%
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    ${(entry.amount / 1000).toFixed(1)}K
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {entry.amountPercentage.toFixed(2)}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Out-of-State Contributions Table */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="flex justify-between items-center mb-4">
          <div>
            <h3 className="text-lg font-semibold">Out-of-State Contributions</h3>
            <p className="text-sm text-gray-600 mt-1">
              {aggregateMode 
                ? `Aggregated donors from outside ${analysis.candidate_state || 'the candidate\'s state'} (grouped by name variations)`
                : `Detailed list of contributions from donors outside ${analysis.candidate_state || 'the candidate\'s state'} for human analysis`
              }
            </p>
          </div>
          <div className="flex gap-2">
            {!showContributions && (
              <>
                <button
                  onClick={() => loadOutOfStateContributions(false)}
                  disabled={loadingContributions}
                  className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed text-sm font-medium"
                >
                  {loadingContributions ? 'Loading...' : 'Load Contributions'}
                </button>
                <button
                  onClick={() => loadOutOfStateContributions(true)}
                  disabled={loadingContributions}
                  className="px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 disabled:bg-gray-400 disabled:cursor-not-allowed text-sm font-medium"
                >
                  {loadingContributions ? 'Loading...' : 'Load Aggregated Donors'}
                </button>
              </>
            )}
            {showContributions && (
              <div className="flex gap-2">
                <button
                  onClick={() => loadOutOfStateContributions(false)}
                  disabled={loadingContributions}
                  className={`px-4 py-2 rounded-md text-sm font-medium ${
                    !aggregateMode
                      ? 'bg-blue-600 text-white hover:bg-blue-700'
                      : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                  } disabled:bg-gray-400 disabled:cursor-not-allowed`}
                >
                  Individual
                </button>
                <button
                  onClick={() => loadOutOfStateContributions(true)}
                  disabled={loadingContributions}
                  className={`px-4 py-2 rounded-md text-sm font-medium ${
                    aggregateMode
                      ? 'bg-green-600 text-white hover:bg-green-700'
                      : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                  } disabled:bg-gray-400 disabled:cursor-not-allowed`}
                >
                  Aggregated
                </button>
              </div>
            )}
          </div>
        </div>

        {contributionsError && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-4">
            <p className="text-red-800">{contributionsError}</p>
          </div>
        )}

        {loadingContributions && (
          <div className="text-center py-8">
            <div className="animate-pulse text-gray-500">Loading contributions...</div>
          </div>
        )}

        {showContributions && !loadingContributions && (
          <>
            <div className="mb-4 flex justify-between items-center">
              <div className="text-sm text-gray-600">
                {aggregateMode ? (
                  <>Showing {aggregatedDonors.length.toLocaleString()} aggregated out-of-state donors (sorted by total amount)</>
                ) : (
                  <>Showing {outOfStateContributions.length.toLocaleString()} out-of-state contributions
                  {analysis.candidate_state && ` (sorted by amount, then date)`}</>
                )}
              </div>
              <button
                onClick={handleExport}
                disabled={exporting || (aggregateMode ? aggregatedDonors.length === 0 : outOfStateContributions.length === 0)}
                className="px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 disabled:bg-gray-400 disabled:cursor-not-allowed text-sm font-medium flex items-center gap-2"
              >
                {exporting ? (
                  <>
                    <svg className="animate-spin h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                    </svg>
                    Exporting...
                  </>
                ) : (
                  <>
                    <svg className="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                    </svg>
                    Export to CSV
                  </>
                )}
              </button>
            </div>
            <div className="overflow-x-auto -mx-6 px-6 max-w-full">
              {aggregateMode ? (
                // Aggregated Donors Table
                <div className="inline-block min-w-full align-middle">
                  <table className="min-w-full divide-y divide-gray-200 table-auto">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider sticky left-0 bg-gray-50 z-10">
                          Donor Name
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Total Amount
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Count
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          State
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider hidden md:table-cell">
                          City
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider hidden lg:table-cell">
                          Employer
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider hidden lg:table-cell">
                          Occupation
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Date Range
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {aggregatedDonors.length === 0 ? (
                        <tr>
                          <td colSpan={8} className="px-3 py-4 text-center text-sm text-gray-500">
                            No aggregated out-of-state donors found
                          </td>
                        </tr>
                      ) : (
                        aggregatedDonors.map((donor, idx) => (
                          <tr key={donor.donor_key || idx} className="hover:bg-gray-50 group">
                            <td className="px-3 py-3 sticky left-0 bg-white group-hover:bg-gray-50 z-10">
                              <div className="text-sm font-medium text-gray-900 max-w-xs truncate" title={donor.canonical_name}>
                                {donor.canonical_name}
                              </div>
                              {donor.all_names && donor.all_names.length > 1 && (
                                <div className="text-xs text-gray-500 mt-1">
                                  {donor.all_names.length - 1} name variation{donor.all_names.length - 1 !== 1 ? 's' : ''}
                                </div>
                              )}
                            </td>
                            <td className="px-3 py-3 whitespace-nowrap text-sm font-medium text-gray-900">
                              ${(donor.total_amount || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                            </td>
                            <td className="px-3 py-3 whitespace-nowrap text-sm text-gray-900">
                              {donor.contribution_count || 0}
                            </td>
                            <td className="px-3 py-3 whitespace-nowrap text-sm text-gray-900">
                              <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-orange-100 text-orange-800">
                                {donor.canonical_state || 'N/A'}
                              </span>
                            </td>
                            <td className="px-3 py-3 whitespace-nowrap text-sm text-gray-500 hidden md:table-cell max-w-xs truncate" title={donor.canonical_city || ''}>
                              {donor.canonical_city || 'N/A'}
                            </td>
                            <td className="px-3 py-3 whitespace-nowrap text-sm text-gray-500 hidden lg:table-cell max-w-xs truncate" title={donor.canonical_employer || ''}>
                              {donor.canonical_employer || 'N/A'}
                            </td>
                            <td className="px-3 py-3 whitespace-nowrap text-sm text-gray-500 hidden lg:table-cell max-w-xs truncate" title={donor.canonical_occupation || ''}>
                              {donor.canonical_occupation || 'N/A'}
                            </td>
                            <td className="px-3 py-3 whitespace-nowrap text-sm text-gray-500">
                              {donor.first_contribution_date && donor.last_contribution_date ? (
                                <div className="text-xs">
                                  <div>{formatDate(donor.first_contribution_date)}</div>
                                  <div className="text-gray-400">to {formatDate(donor.last_contribution_date)}</div>
                                </div>
                              ) : donor.first_contribution_date ? (
                                formatDate(donor.first_contribution_date)
                              ) : (
                                'N/A'
                              )}
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              ) : (
                // Individual Contributions Table
                <div className="inline-block min-w-full align-middle">
                  <table className="min-w-full divide-y divide-gray-200 table-auto">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Date
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Amount
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider sticky left-0 bg-gray-50 z-10">
                          Donor Name
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          State
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider hidden md:table-cell">
                          City
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider hidden lg:table-cell">
                          Employer
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider hidden lg:table-cell">
                          Occupation
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {outOfStateContributions.length === 0 ? (
                        <tr>
                          <td colSpan={7} className="px-3 py-4 text-center text-sm text-gray-500">
                            No out-of-state contributions found
                          </td>
                        </tr>
                      ) : (
                        outOfStateContributions.map((contrib, idx) => (
                          <tr key={contrib.contribution_id || idx} className="hover:bg-gray-50 group">
                            <td className="px-3 py-3 whitespace-nowrap text-sm text-gray-900">
                              {formatDate(contrib.contribution_date || contrib.contribution_receipt_date)}
                            </td>
                            <td className="px-3 py-3 whitespace-nowrap text-sm font-medium text-gray-900">
                              ${(contrib.contribution_amount || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                            </td>
                            <td className="px-3 py-3 sticky left-0 bg-white group-hover:bg-gray-50 z-10">
                              <div className="text-sm text-gray-900 max-w-xs truncate" title={contrib.contributor_name || ''}>
                                {contrib.contributor_name || 'N/A'}
                              </div>
                            </td>
                            <td className="px-3 py-3 whitespace-nowrap text-sm text-gray-900">
                              <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-orange-100 text-orange-800">
                                {contrib.contributor_state || 'N/A'}
                              </span>
                            </td>
                            <td className="px-3 py-3 whitespace-nowrap text-sm text-gray-500 hidden md:table-cell max-w-xs truncate" title={contrib.contributor_city || ''}>
                              {contrib.contributor_city || 'N/A'}
                            </td>
                            <td className="px-3 py-3 whitespace-nowrap text-sm text-gray-500 hidden lg:table-cell max-w-xs truncate" title={contrib.contributor_employer || ''}>
                              {contrib.contributor_employer || 'N/A'}
                            </td>
                            <td className="px-3 py-3 whitespace-nowrap text-sm text-gray-500 hidden lg:table-cell max-w-xs truncate" title={contrib.contributor_occupation || ''}>
                              {contrib.contributor_occupation || 'N/A'}
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

