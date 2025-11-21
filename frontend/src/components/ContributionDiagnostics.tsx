import { useEffect, useState } from 'react';
import { contributionApi, ContributionDiagnostics as ContributionDiagnosticsType } from '../services/api';
import { useNavigate } from 'react-router-dom';
import { formatDate } from '../utils/dateUtils';

interface ContributionDiagnosticsProps {
  candidateId: string;
  cycle?: number;
}

export default function ContributionDiagnostics({
  candidateId,
  cycle,
}: ContributionDiagnosticsProps) {
  const [diagnostics, setDiagnostics] = useState<ContributionDiagnosticsType | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  useEffect(() => {
    const fetchDiagnostics = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await contributionApi.getDiagnostics({
          candidate_id: candidateId,
          cycle,
        });
        setDiagnostics(data);
      } catch (err: any) {
        const errorMessage = err?.response?.data?.detail || err?.message || 'Unknown error';
        setError(errorMessage);
      } finally {
        setLoading(false);
      }
    };

    if (candidateId) {
      fetchDiagnostics();
    }
  }, [candidateId, cycle]);

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="animate-pulse">
          <div className="h-4 bg-gray-200 rounded w-3/4 mb-4"></div>
          <div className="h-4 bg-gray-200 rounded w-1/2"></div>
        </div>
      </div>
    );
  }

  if (error || !diagnostics) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Contribution Data Diagnostics</h2>
        <div className="text-red-600">
          {error || 'Failed to load diagnostics'}
        </div>
      </div>
    );
  }

  const needsData = diagnostics.total_contributions === 0;
  const recommendationColor = needsData ? 'text-yellow-600' : 'text-green-600';
  const recommendationBg = needsData ? 'bg-yellow-50' : 'bg-green-50';

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Contribution Data Diagnostics</h2>
      
      <div className="space-y-4">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div>
            <div className="text-sm text-gray-600">Total Contributions</div>
            <div className="text-2xl font-bold">{diagnostics.total_contributions.toLocaleString()}</div>
          </div>
          {diagnostics.cycle && (
            <div>
              <div className="text-sm text-gray-600">Cycle</div>
              <div className="text-2xl font-bold">{diagnostics.cycle}</div>
            </div>
          )}
          <div>
            <div className="text-sm text-gray-600">Available Cycles</div>
            <div className="text-2xl font-bold">{diagnostics.bulk_data_available_cycles.length}</div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Status</div>
            <div className={`text-lg font-semibold ${recommendationColor}`}>
              {diagnostics.total_contributions > 0 ? 'Data Available' : 'No Data'}
            </div>
          </div>
        </div>

        {diagnostics.date_range.min_date && diagnostics.date_range.max_date && (
          <div>
            <div className="text-sm text-gray-600 mb-1">Date Range</div>
            <div className="text-gray-900">
              {formatDate(diagnostics.date_range.min_date)} -{' '}
              {formatDate(diagnostics.date_range.max_date)}
            </div>
          </div>
        )}

        {Object.keys(diagnostics.contributions_by_cycle).length > 0 && (
          <div>
            <div className="text-sm text-gray-600 mb-2">Contributions by Cycle</div>
            <div className="flex flex-wrap gap-2">
              {Object.entries(diagnostics.contributions_by_cycle).map(([cycle, count]) => (
                <span key={cycle} className="px-3 py-1 bg-blue-100 text-blue-800 rounded-full text-sm">
                  {cycle}: {count.toLocaleString()}
                </span>
              ))}
            </div>
          </div>
        )}

        {diagnostics.bulk_data_available_cycles.length > 0 && (
          <div>
            <div className="text-sm text-gray-600 mb-2">Bulk Data Available Cycles</div>
            <div className="flex flex-wrap gap-2">
              {diagnostics.bulk_data_available_cycles.map((cycle) => (
                <span key={cycle} className="px-3 py-1 bg-green-100 text-green-800 rounded-full text-sm">
                  {cycle}
                </span>
              ))}
            </div>
          </div>
        )}

        <div className={`${recommendationBg} p-4 rounded-lg`}>
          <div className="font-semibold mb-2">Recommendation</div>
          <div className={recommendationColor}>{diagnostics.recommendation_message}</div>
        </div>

        {needsData && diagnostics.bulk_data_available_cycles.length > 0 && (
          <div className="pt-4 border-t">
            <button
              onClick={() => navigate('/bulk-data')}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
            >
              Load Bulk Data
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

