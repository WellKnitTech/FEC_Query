import { useState } from 'react';
import { useParams } from 'react-router-dom';
import { candidateApi } from '../services/api';
import { formatDateTime } from '../utils/dateUtils';
import { useCandidateData } from '../hooks/useCandidateData';
import { useFinancialData } from '../hooks/useFinancialData';
import { useCycleSelector } from '../hooks/useCycleSelector';
import { CandidateContextProvider } from '../contexts/CandidateContext';
import FinancialSummary from '../components/FinancialSummary';
import DonorStateAnalysis from '../components/DonorStateAnalysis';
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
import LoadingState from '../components/candidate/LoadingState';
import ErrorState from '../components/candidate/ErrorState';

function CandidateDetailContent() {
  const { candidateId } = useParams<{ candidateId: string }>();
  const { candidate, loading, error, refresh: refreshCandidate } = useCandidateData(candidateId);
  const [refreshingContactInfo, setRefreshingContactInfo] = useState(false);

  // Fetch financials to determine available cycles (fetch all, no cycle filter)
  const { financials, latestFinancial, selectedFinancial, availableCycles } = useFinancialData(
    candidateId,
    undefined // Fetch all financials to determine available cycles
  );

  // Manage cycle selection - start with latest cycle if available
  const { selectedCycle, setCycle } = useCycleSelector({
    financials,
    initialCycle: latestFinancial?.cycle,
    onCycleChange: undefined, // Cycle changes will update context via FinancialSummary's onCycleChange
  });

  const handleRefreshContactInfo = async () => {
    if (!candidateId) return;
    
    setRefreshingContactInfo(true);
    try {
      const result = await candidateApi.refreshContactInfo(candidateId);
      if (result.success) {
        // Refresh candidate data to get updated contact info
        await refreshCandidate();
      }
    } catch (err: any) {
      console.error('Error refreshing contact info:', err);
    } finally {
      setRefreshingContactInfo(false);
    }
  };


  if (loading) {
    return (
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <LoadingState message="Loading candidate information..." />
      </div>
    );
  }

  if (error || !candidate) {
    return (
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <ErrorState
          title={error ? 'Error Loading Candidate' : 'Candidate Not Found'}
          error={error || (!candidate ? 'The candidate you\'re looking for could not be found. Please check the candidate ID and try again.' : null)}
          onRetry={error ? refreshCandidate : undefined}
        />
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

      {/* Contact Information Section - Always show if candidate exists */}
      <div className="bg-white rounded-lg shadow p-6 mb-6">
        <div className="flex justify-between items-start mb-4">
          <h2 className="text-xl font-semibold text-gray-900">Contact Information</h2>
          <button
            onClick={handleRefreshContactInfo}
            disabled={refreshingContactInfo}
            className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed text-sm font-medium"
          >
            {refreshingContactInfo ? 'Refreshing...' : 'Refresh Contact Info'}
          </button>
        </div>
        
        {candidate.contact_info && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
            {candidate.contact_info.street_address && (
              <div>
                <span className="text-sm font-medium text-gray-500">Address:</span>
                <p className="text-gray-900">
                  {candidate.contact_info.street_address}
                  {candidate.contact_info.city && `, ${candidate.contact_info.city}`}
                  {candidate.contact_info.state && `, ${candidate.contact_info.state}`}
                  {candidate.contact_info.zip && ` ${candidate.contact_info.zip}`}
                </p>
              </div>
            )}
            {candidate.contact_info.email && (
              <div>
                <span className="text-sm font-medium text-gray-500">Email:</span>
                <p className="text-gray-900">
                  <a href={`mailto:${candidate.contact_info.email}`} className="text-blue-600 hover:underline">
                    {candidate.contact_info.email}
                  </a>
                </p>
              </div>
            )}
            {candidate.contact_info.phone && (
              <div>
                <span className="text-sm font-medium text-gray-500">Phone:</span>
                <p className="text-gray-900">
                  <a href={`tel:${candidate.contact_info.phone}`} className="text-blue-600 hover:underline">
                    {candidate.contact_info.phone}
                  </a>
                </p>
              </div>
            )}
            {candidate.contact_info.website && (
              <div>
                <span className="text-sm font-medium text-gray-500">Website:</span>
                <p className="text-gray-900">
                  <a 
                    href={candidate.contact_info.website} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    className="text-blue-600 hover:underline"
                  >
                    {candidate.contact_info.website}
                  </a>
                </p>
              </div>
            )}
          </div>
        )}
        
        {!candidate.contact_info && (
          <p className="text-gray-500 mb-4">No contact information available. Click "Refresh Contact Info" to fetch from the FEC API.</p>
        )}
        
        <div className="text-sm text-gray-500 border-t pt-4">
          Last updated: {formatDateTime(candidate.contact_info_updated_at)}
        </div>
      </div>

      <CandidateContextProvider
        candidateId={candidateId}
        candidate={candidate}
        cycle={selectedCycle}
        setCycle={setCycle}
        financials={financials}
        latestFinancial={latestFinancial}
        selectedFinancial={selectedFinancial}
        availableCycles={availableCycles}
      >
        <div className="space-y-6">
          <FinancialSummary 
            candidateId={candidateId!} 
            cycle={selectedCycle} 
            onCycleChange={(newCycle) => {
              setCycle(newCycle);
            }} 
          />
          <DonorStateAnalysis candidateId={candidateId} candidate={candidate} cycle={selectedCycle} />
          <ContributionAnalysis candidateId={candidateId} cycle={selectedCycle} />
          <CumulativeChart candidateId={candidateId} cycle={selectedCycle} />
          <ContributionVelocity candidateId={candidateId} cycle={selectedCycle} />
          <EmployerTreemap candidateId={candidateId} cycle={selectedCycle} />
          <ExpenditureBreakdown candidateId={candidateId} cycle={selectedCycle} />
          <NetworkGraph candidateId={candidateId!} />
          <FraudRadarChart candidateId={candidateId!} />
          <SmurfingScatter candidateId={candidateId} />
          <FraudAlerts candidateId={candidateId!} />
        </div>
      </CandidateContextProvider>
    </div>
  );
}

export default function CandidateDetail() {
  return <CandidateDetailContent />;
}

