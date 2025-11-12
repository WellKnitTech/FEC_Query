import { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { candidateApi, Candidate } from '../services/api';
import FinancialSummary from '../components/FinancialSummary';
import ContributionAnalysis from '../components/ContributionAnalysis';
import NetworkGraph from '../components/NetworkGraph';
import FraudAlerts from '../components/FraudAlerts';
import ExpenditureBreakdown from '../components/ExpenditureBreakdown';
import EmployerTreemap from '../components/EmployerTreemap';
import ContributionVelocity from '../components/ContributionVelocity';
import CumulativeChart from '../components/CumulativeChart';
import FraudRadarChart from '../components/FraudRadarChart';
import SmurfingScatter from '../components/SmurfingScatter';
import ExportButton from '../components/ExportButton';

export default function CandidateDetail() {
  const { candidateId } = useParams<{ candidateId: string }>();
  const [candidate, setCandidate] = useState<Candidate | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchCandidate = async () => {
      if (!candidateId) return;
      
      setLoading(true);
      setError(null);
      try {
        const data = await candidateApi.getById(candidateId);
        setCandidate(data);
      } catch (err: any) {
        setError(err?.response?.data?.detail || err?.message || 'Failed to load candidate data');
      } finally {
        setLoading(false);
      }
    };

    fetchCandidate();
  }, [candidateId]);

  if (loading) {
    return (
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-200 rounded w-1/2 mb-4"></div>
          <div className="h-4 bg-gray-200 rounded w-1/3"></div>
        </div>
      </div>
    );
  }

  if (error || !candidate) {
    return (
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {error && <div className="text-red-600">{error}</div>}
        {!candidate && !error && <div>Candidate not found</div>}
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div className="mb-6">
        <div className="flex justify-between items-start mb-2">
          <h1 className="text-3xl font-bold text-gray-900">{candidate.name}</h1>
          {candidateId && <ExportButton candidateId={candidateId} />}
        </div>
        <div className="flex gap-4 text-gray-600">
          {candidate.office && (
            <span>
              Office: {candidate.office}
              {candidate.state && ` (${candidate.state})`}
            </span>
          )}
          {candidate.party && <span>Party: {candidate.party}</span>}
          {candidate.election_years && candidate.election_years.length > 0 && (
            <span>Elections: {candidate.election_years.join(', ')}</span>
          )}
        </div>
      </div>

      <div className="space-y-6">
        <FinancialSummary candidateId={candidateId!} />
        <ContributionAnalysis candidateId={candidateId} />
        <CumulativeChart candidateId={candidateId} />
        <ContributionVelocity candidateId={candidateId} />
        <EmployerTreemap candidateId={candidateId} />
        <ExpenditureBreakdown candidateId={candidateId} />
        <NetworkGraph candidateId={candidateId!} />
        <FraudRadarChart candidateId={candidateId!} />
        <SmurfingScatter candidateId={candidateId} />
        <FraudAlerts candidateId={candidateId!} />
      </div>
    </div>
  );
}

