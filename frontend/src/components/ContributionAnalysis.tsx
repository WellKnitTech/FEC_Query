import { useEffect, useState } from 'react';
import { contributionApi, ContributionAnalysis as ContributionAnalysisType } from '../services/api';
import { Line, Bar, Doughnut } from 'react-chartjs-2';
import AnalysisSection from './candidate/AnalysisSection';
import { formatCurrency } from '../utils/candidateCalculations';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
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
  ArcElement,
  Title,
  Tooltip,
  Legend
);

interface ContributionAnalysisProps {
  candidateId?: string;
  committeeId?: string;
  minDate?: string;
  maxDate?: string;
  cycle?: number;
  refreshToken?: number;
}

export default function ContributionAnalysis({
  candidateId,
  committeeId,
  minDate,
  maxDate,
  cycle,
  refreshToken = 0,
}: ContributionAnalysisProps) {
  const [analysis, setAnalysis] = useState<ContributionAnalysisType | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [fetching, setFetching] = useState(false);
  const [fetchMessage, setFetchMessage] = useState<string | null>(null);

  useEffect(() => {
    if (!candidateId && !committeeId) {
      return;
    }

    const abortController = new AbortController();
    
    const fetchAnalysis = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await contributionApi.analyze({
          candidate_id: candidateId,
          committee_id: committeeId,
          min_date: minDate,
          max_date: maxDate,
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
        const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to load contribution analysis';
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
  }, [candidateId, committeeId, minDate, maxDate, cycle, refreshToken]);

  const refresh = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await contributionApi.analyze({
        candidate_id: candidateId,
        committee_id: committeeId,
        min_date: minDate,
        max_date: maxDate,
        cycle: cycle,
      });
      setAnalysis(data);
    } catch (err: any) {
      const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to load contribution analysis';
      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  const fetchFromApi = async () => {
    if (!candidateId && !committeeId) {
      return;
    }

    setFetching(true);
    setFetchMessage(null);
    setError(null);
    
    try {
      const result = await contributionApi.fetchFromApi({
        candidate_id: candidateId,
        committee_id: committeeId,
        cycle: cycle,
      });
      
      if (result.success) {
        setFetchMessage(
          `Successfully fetched ${result.fetched_count} contributions and stored ${result.stored_count} new records.`
        );
        
        // Update analysis with the new results from the API
        if (result.analysis) {
          setAnalysis(result.analysis);
        } else {
          // If analysis wasn't returned, refresh it manually
          await refresh();
        }
        
        // Clear the message after 5 seconds
        setTimeout(() => setFetchMessage(null), 5000);
      }
    } catch (err: any) {
      const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to fetch contributions from API';
      setError(errorMessage);
    } finally {
      setFetching(false);
    }
  };

  return (
    <AnalysisSection
      title="Contribution Analysis"
      loading={loading}
      error={error}
      onRetry={refresh}
    >
      {analysis && (() => {
        // Prepare chart data
        const dateLabels = Object.keys(analysis.contributions_by_date || {}).sort();
        const dateData = dateLabels.map((date) => analysis.contributions_by_date[date] || 0);

        const stateLabels = Object.keys(analysis.contributions_by_state || {});
        const stateData = stateLabels.map((state) => analysis.contributions_by_state[state] || 0);

        const distributionLabels = Object.keys(analysis.contribution_distribution || {});
        const distributionData = Object.values(analysis.contribution_distribution || {});

        const topDonors = analysis.top_donors || [];
        const topDonorsData = {
          labels: topDonors.slice(0, 10).map((d) => d.name || 'Unknown'),
          datasets: [
            {
              label: 'Total Contributions',
              data: topDonors.slice(0, 10).map((d) => d.total || 0),
              backgroundColor: 'rgba(59, 130, 246, 0.5)',
              borderColor: 'rgba(59, 130, 246, 1)',
              borderWidth: 1,
            },
          ],
        };

        const dateChartData = {
          labels: dateLabels,
          datasets: [
            {
              label: 'Contributions by Date',
              data: dateData,
              borderColor: 'rgba(59, 130, 246, 1)',
              backgroundColor: 'rgba(59, 130, 246, 0.1)',
              tension: 0.1,
            },
          ],
        };

        const stateChartData = {
          labels: stateLabels.slice(0, 10),
          datasets: [
            {
              label: 'Contributions by State',
              data: stateData.slice(0, 10),
              backgroundColor: 'rgba(16, 185, 129, 0.5)',
              borderColor: 'rgba(16, 185, 129, 1)',
              borderWidth: 1,
            },
          ],
        };

        const distributionChartData = {
          labels: distributionLabels,
          datasets: [
            {
              data: distributionData,
              backgroundColor: [
                'rgba(239, 68, 68, 0.5)',
                'rgba(245, 158, 11, 0.5)',
                'rgba(251, 191, 36, 0.5)',
                'rgba(34, 197, 94, 0.5)',
                'rgba(59, 130, 246, 0.5)',
                'rgba(139, 92, 246, 0.5)',
                'rgba(236, 72, 153, 0.5)',
              ],
              borderWidth: 1,
            },
          ],
        };

        return (
          <div className="space-y-6">
            {/* Success Message from API Fetch */}
            {fetchMessage && (
              <div className="p-4 rounded-lg border bg-green-50 border-green-200">
                <div className="flex items-start">
                  <div className="flex-shrink-0">
                    <svg className="h-5 w-5 text-green-600" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                    </svg>
                  </div>
                  <div className="ml-3 flex-1">
                    <p className="text-sm text-green-800">{fetchMessage}</p>
                  </div>
                </div>
              </div>
            )}

            {/* Warning Message (from backend) */}
            {analysis.warning_message && (
              <div className={`p-4 rounded-lg border ${
                analysis.using_financial_totals_fallback
                  ? 'bg-blue-50 border-blue-200'
                  : 'bg-yellow-50 border-yellow-200'
              }`}>
                <div className="flex items-start">
                  <div className="flex-shrink-0">
                    {analysis.using_financial_totals_fallback ? (
                      <svg className="h-5 w-5 text-blue-600" fill="currentColor" viewBox="0 0 20 20">
                        <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
                      </svg>
                    ) : (
                      <svg className="h-5 w-5 text-yellow-600" fill="currentColor" viewBox="0 0 20 20">
                        <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
                      </svg>
                    )}
                  </div>
                  <div className="ml-3 flex-1">
                    <h3 className={`text-sm font-medium ${
                      analysis.using_financial_totals_fallback ? 'text-blue-800' : 'text-yellow-800'
                    }`}>
                      {analysis.using_financial_totals_fallback ? 'Estimated from Financial Totals' : 'Partial Data Analysis'}
                    </h3>
                    <div className={`mt-2 text-sm ${
                      analysis.using_financial_totals_fallback ? 'text-blue-700' : 'text-yellow-700'
                    }`}>
                      <p>{analysis.warning_message}</p>
                      {analysis.using_financial_totals_fallback && analysis.total_from_api && (
                        <p className="mt-2 font-medium">
                          Estimated Total: {formatCurrency(analysis.total_from_api)}
                        </p>
                      )}
                    </div>
                    {/* Fetch from API Button */}
                    {(analysis.using_financial_totals_fallback || 
                      (analysis.data_completeness !== null && analysis.data_completeness !== undefined && analysis.data_completeness < 50)) && (
                      <div className="mt-4">
                        <button
                          onClick={fetchFromApi}
                          disabled={fetching}
                          className={`inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white ${
                            fetching
                              ? 'bg-gray-400 cursor-not-allowed'
                              : 'bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500'
                          }`}
                        >
                          {fetching ? (
                            <>
                              <svg className="animate-spin -ml-1 mr-2 h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                              </svg>
                              Fetching...
                            </>
                          ) : (
                            <>
                              <svg className="-ml-1 mr-2 h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                              </svg>
                              Fetch from API
                            </>
                          )}
                        </button>
                        <p className="mt-2 text-xs text-gray-600">
                          This will fetch all available contributions from the FEC API and update the analysis automatically.
                        </p>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )}
            
            {/* Data Completeness Warning (fallback for older format) */}
            {!analysis.warning_message && analysis.data_completeness !== null && analysis.data_completeness !== undefined && analysis.data_completeness < 100 && (
              <div className={`p-4 rounded-lg border ${
                analysis.data_completeness < 50 
                  ? 'bg-yellow-50 border-yellow-200' 
                  : 'bg-blue-50 border-blue-200'
              }`}>
                <div className="flex items-start">
                  <div className="flex-shrink-0">
                    {analysis.data_completeness < 50 ? (
                      <svg className="h-5 w-5 text-yellow-600" fill="currentColor" viewBox="0 0 20 20">
                        <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
                      </svg>
                    ) : (
                      <svg className="h-5 w-5 text-blue-600" fill="currentColor" viewBox="0 0 20 20">
                        <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
                      </svg>
                    )}
                  </div>
                  <div className="ml-3 flex-1">
                    <h3 className={`text-sm font-medium ${
                      analysis.data_completeness < 50 ? 'text-yellow-800' : 'text-blue-800'
                    }`}>
                      Partial Data Analysis
                    </h3>
                    <div className={`mt-2 text-sm ${
                      analysis.data_completeness < 50 ? 'text-yellow-700' : 'text-blue-700'
                    }`}>
                      <p>
                        This analysis is based on {analysis.data_completeness.toFixed(1)}% of total contributions.
                        {analysis.total_from_api && (
                          <> Local database: {formatCurrency(analysis.total_contributions || 0)}. FEC API total: {formatCurrency(analysis.total_from_api)}.</>
                        )}
                        {analysis.data_completeness < 50 && (
                          <> Consider importing additional bulk data for complete analysis.</>
                        )}
                      </p>
                      {/* Fetch from API Button for low completeness */}
                      {analysis.data_completeness < 50 && (
                        <div className="mt-4">
                          <button
                            onClick={fetchFromApi}
                            disabled={fetching}
                            className={`inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white ${
                              fetching
                                ? 'bg-gray-400 cursor-not-allowed'
                                : 'bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500'
                            }`}
                          >
                            {fetching ? (
                              <>
                                <svg className="animate-spin -ml-1 mr-2 h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                                </svg>
                                Fetching...
                              </>
                            ) : (
                              <>
                                <svg className="-ml-1 mr-2 h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                                </svg>
                                Fetch from API
                              </>
                            )}
                          </button>
                          <p className="mt-2 text-xs text-gray-600">
                            This will fetch all available contributions from the FEC API and update the analysis automatically.
                          </p>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            )}
            
            {/* Show indicator when using financial totals fallback */}
            {analysis.using_financial_totals_fallback && (
              <div className="mb-4 text-sm text-gray-600 italic">
                * Values shown are estimates based on financial totals from FEC filings. Detailed donor information will appear once individual contribution records are published.
              </div>
            )}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
              <div>
                <div className="text-sm text-gray-600">Total Contributions</div>
                <div className="text-2xl font-bold">
                  {formatCurrency(analysis.total_contributions || 0)}
                </div>
              </div>
              <div>
                <div className="text-sm text-gray-600">Total Contributors</div>
                <div className="text-2xl font-bold">{(analysis.total_contributors || 0).toLocaleString()}</div>
              </div>
              <div>
                <div className="text-sm text-gray-600">Average Contribution</div>
                <div className="text-2xl font-bold">${(analysis.average_contribution || 0).toFixed(2)}</div>
              </div>
              <div>
                <div className="text-sm text-gray-600">Top Donors</div>
                <div className="text-2xl font-bold">{(analysis.top_donors || []).length}</div>
              </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-lg font-semibold mb-4">Contributions Over Time</h3>
                <Line data={dateChartData} options={{ responsive: true, maintainAspectRatio: true }} />
              </div>

              <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-lg font-semibold mb-4">Top States</h3>
                <Bar data={stateChartData} options={{ responsive: true, maintainAspectRatio: true }} />
              </div>

              <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-lg font-semibold mb-4">Top Donors</h3>
                <Bar data={topDonorsData} options={{ responsive: true, maintainAspectRatio: true, indexAxis: 'y' }} />
              </div>

              <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-lg font-semibold mb-4">Contribution Distribution</h3>
                <Doughnut data={distributionChartData} options={{ responsive: true, maintainAspectRatio: true }} />
              </div>
            </div>
          </div>
        );
      })()}
    </AnalysisSection>
  );
}

