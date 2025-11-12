import { useEffect, useState } from 'react';
import { candidateApi, FinancialSummary } from '../services/api';

interface FinancialSummaryProps {
  candidateId: string;
}

export default function FinancialSummaryComponent({ candidateId }: FinancialSummaryProps) {
  const [financials, setFinancials] = useState<FinancialSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchFinancials = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await candidateApi.getFinancials(candidateId);
        setFinancials(data);
      } catch (err: any) {
        setError(err?.response?.data?.detail || err?.message || 'Failed to load financial data');
      } finally {
        setLoading(false);
      }
    };

    if (candidateId) {
      fetchFinancials();
    }
  }, [candidateId]);

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

  if (error) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="text-red-600">{error}</div>
      </div>
    );
  }

  if (financials.length === 0) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Financial Summary</h2>
        <p className="text-gray-600">No financial data available</p>
      </div>
    );
  }

  const latest = financials[0];

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Financial Summary (Cycle {latest.cycle})</h2>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="bg-blue-50 p-4 rounded-lg">
          <div className="text-sm text-gray-600 mb-1">Total Receipts</div>
          <div className="text-2xl font-bold text-blue-900">
            ${(latest.total_receipts / 1000).toFixed(1)}K
          </div>
        </div>
        <div className="bg-green-50 p-4 rounded-lg">
          <div className="text-sm text-gray-600 mb-1">Cash on Hand</div>
          <div className="text-2xl font-bold text-green-900">
            ${(latest.cash_on_hand / 1000).toFixed(1)}K
          </div>
        </div>
        <div className="bg-red-50 p-4 rounded-lg">
          <div className="text-sm text-gray-600 mb-1">Total Disbursements</div>
          <div className="text-2xl font-bold text-red-900">
            ${(latest.total_disbursements / 1000).toFixed(1)}K
          </div>
        </div>
        <div className="bg-purple-50 p-4 rounded-lg">
          <div className="text-sm text-gray-600 mb-1">Total Contributions</div>
          <div className="text-2xl font-bold text-purple-900">
            ${(latest.total_contributions / 1000).toFixed(1)}K
          </div>
        </div>
      </div>
      <div className="mt-6 grid grid-cols-3 gap-4">
        <div>
          <div className="text-sm text-gray-600">Individual Contributions</div>
          <div className="text-lg font-semibold">
            ${(latest.individual_contributions / 1000).toFixed(1)}K
          </div>
        </div>
        <div>
          <div className="text-sm text-gray-600">PAC Contributions</div>
          <div className="text-lg font-semibold">
            ${(latest.pac_contributions / 1000).toFixed(1)}K
          </div>
        </div>
        <div>
          <div className="text-sm text-gray-600">Party Contributions</div>
          <div className="text-lg font-semibold">
            ${(latest.party_contributions / 1000).toFixed(1)}K
          </div>
        </div>
      </div>
    </div>
  );
}

