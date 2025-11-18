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
}

export default function ContributionAnalysis({
  candidateId,
  committeeId,
  minDate,
  maxDate,
  cycle,
}: ContributionAnalysisProps) {
  const [analysis, setAnalysis] = useState<ContributionAnalysisType | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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
          console.error('Error loading contribution analysis:', err);
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
  }, [candidateId, committeeId, minDate, maxDate, cycle]);

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
      console.error('Error loading contribution analysis:', err);
    } finally {
      setLoading(false);
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
            {/* Data Completeness Warning */}
            {analysis.data_completeness !== null && analysis.data_completeness !== undefined && analysis.data_completeness < 100 && (
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
                    </div>
                  </div>
                </div>
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

