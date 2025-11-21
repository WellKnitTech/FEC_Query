import { useState, useMemo, lazy, Suspense } from 'react';
import { useParams } from 'react-router-dom';
import { candidateApi } from '../services/api';
import { formatDateTime, cycleToDateRange, formatCycleRange, formatDate } from '../utils/dateUtils';
import { useCandidateData } from '../hooks/useCandidateData';
import { useFinancialData } from '../hooks/useFinancialData';
import { useCycleSelector } from '../hooks/useCycleSelector';
import { CandidateContextProvider } from '../contexts/CandidateContext';
import FinancialSummary from '../components/FinancialSummary';
import DonorStateAnalysis from '../components/DonorStateAnalysis';
import ContributionAnalysis from '../components/ContributionAnalysis';
import ExportButton from '../components/ExportButton';
import LoadingState from '../components/candidate/LoadingState';
import ErrorState from '../components/candidate/ErrorState';
import ErrorBoundary from '../components/candidate/ErrorBoundary';

// Lazy load heavy visualization components
const NetworkGraph = lazy(() => import('../components/NetworkGraph'));
const FraudAlerts = lazy(() => import('../components/FraudAlerts'));
const ExpenditureBreakdown = lazy(() => import('../components/ExpenditureBreakdown'));
const EmployerTreemap = lazy(() => import('../components/EmployerTreemap'));
const ContributionVelocity = lazy(() => import('../components/ContributionVelocity'));
const CumulativeChart = lazy(() => import('../components/CumulativeChart'));
const FraudRadarChart = lazy(() => import('../components/FraudRadarChart'));
const SmurfingScatter = lazy(() => import('../components/SmurfingScatter'));

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
  const { selectedCycle, setCycle, hasMultipleCycles } = useCycleSelector({
    financials,
    initialCycle: latestFinancial?.cycle,
    onCycleChange: undefined, // Cycle changes will update context via FinancialSummary's onCycleChange
  });

  // Calculate date range from selected cycle
  const cycleDateRange = useMemo(() => {
    if (selectedCycle) {
      return cycleToDateRange(selectedCycle);
    }
    return { minDate: undefined, maxDate: undefined };
  }, [selectedCycle]);

  const handleRefreshContactInfo = async () => {
    if (!candidateId) return;
    
    setRefreshingContactInfo(true);
    try {
      console.log(`Refreshing contact info for candidate ${candidateId}`);
      const result = await candidateApi.refreshContactInfo(candidateId);
      console.log('Refresh result:', result);
      if (result.success) {
        // Refresh candidate data to get updated contact info
        await refreshCandidate();
      } else {
        console.warn('Contact info refresh returned success=false:', result);
      }
    } catch (err: any) {
      console.error('Error refreshing contact info:', err);
      // Show error to user with more helpful message
      let errorMessage = 'Unknown error';
      if (err?.code === 'ECONNABORTED' || err?.message?.includes('timeout')) {
        errorMessage = 'The request timed out. The FEC API may be slow or unreachable. Please try again in a few moments.';
      } else if (err?.response?.status === 504) {
        errorMessage = err?.response?.data?.detail || 'The server timed out while fetching contact information. Please try again later.';
      } else if (err?.response?.data?.detail) {
        errorMessage = err.response.data.detail;
      } else if (err?.message) {
        errorMessage = err.message;
      }
      alert(`Failed to refresh contact info: ${errorMessage}`);
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
        {/* Cycle Indicator Banner */}
        {selectedCycle && (
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-6">
            <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
              <div className="flex flex-col gap-1">
                <div className="text-sm font-medium text-blue-900">Election Cycle</div>
                <div className="text-xl font-bold text-blue-900">
                  Cycle {selectedCycle}
                </div>
                {cycleDateRange.minDate && cycleDateRange.maxDate && (
                  <div className="text-sm text-blue-700">
                    {formatDate(cycleDateRange.minDate)} - {formatDate(cycleDateRange.maxDate)}
                  </div>
                )}
              </div>
              {hasMultipleCycles && availableCycles.length > 0 && (
                <div className="flex items-center gap-2">
                  <label htmlFor="page-cycle-select" className="text-sm font-medium text-blue-900">
                    Switch Cycle:
                  </label>
                  <select
                    id="page-cycle-select"
                    value={selectedCycle}
                    onChange={(e) => {
                      const newCycle = e.target.value ? parseInt(e.target.value, 10) : undefined;
                      setCycle(newCycle);
                    }}
                    className="px-3 py-2 border border-blue-300 rounded-md text-sm bg-white text-gray-900 focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  >
                    {availableCycles.map((c) => (
                      <option key={c} value={c}>
                        {c}
                      </option>
                    ))}
                  </select>
                </div>
              )}
            </div>
          </div>
        )}

        <div className="space-y-6">
          {/* Critical components - load immediately with error boundaries */}
          <ErrorBoundary>
            <FinancialSummary 
              candidateId={candidateId!} 
              cycle={selectedCycle} 
              onCycleChange={(newCycle) => {
                setCycle(newCycle);
              }} 
            />
          </ErrorBoundary>
          <ErrorBoundary>
            <DonorStateAnalysis candidateId={candidateId} candidate={candidate} cycle={selectedCycle} />
          </ErrorBoundary>
          <ErrorBoundary>
            <ContributionAnalysis candidateId={candidateId} cycle={selectedCycle} />
          </ErrorBoundary>
          
          {/* Deferred visualizations - lazy loaded with suspense and error boundaries */}
          {selectedCycle && (
            <>
              <ErrorBoundary>
                <Suspense fallback={<div className="bg-white rounded-lg shadow p-6"><div className="animate-pulse"><div className="h-64 bg-gray-200 rounded"></div></div></div>}>
                  <CumulativeChart candidateId={candidateId} cycle={selectedCycle} />
                </Suspense>
              </ErrorBoundary>
              <ErrorBoundary>
                <Suspense fallback={<div className="bg-white rounded-lg shadow p-6"><div className="animate-pulse"><div className="h-64 bg-gray-200 rounded"></div></div></div>}>
                  <ContributionVelocity candidateId={candidateId} cycle={selectedCycle} />
                </Suspense>
              </ErrorBoundary>
              <ErrorBoundary>
                <Suspense fallback={<div className="bg-white rounded-lg shadow p-6"><div className="animate-pulse"><div className="h-64 bg-gray-200 rounded"></div></div></div>}>
                  <EmployerTreemap candidateId={candidateId} cycle={selectedCycle} />
                </Suspense>
              </ErrorBoundary>
              <ErrorBoundary>
                <Suspense fallback={<div className="bg-white rounded-lg shadow p-6"><div className="animate-pulse"><div className="h-64 bg-gray-200 rounded"></div></div></div>}>
                  <ExpenditureBreakdown candidateId={candidateId} cycle={selectedCycle} />
                </Suspense>
              </ErrorBoundary>
              <ErrorBoundary>
                <Suspense fallback={<div className="bg-white rounded-lg shadow p-6"><div className="animate-pulse"><div className="h-96 bg-gray-200 rounded"></div></div></div>}>
                  <NetworkGraph 
                    candidateId={candidateId!} 
                    minDate={cycleDateRange.minDate}
                    maxDate={cycleDateRange.maxDate}
                  />
                </Suspense>
              </ErrorBoundary>
              <ErrorBoundary>
                <Suspense fallback={<div className="bg-white rounded-lg shadow p-6"><div className="animate-pulse"><div className="h-64 bg-gray-200 rounded"></div></div></div>}>
                  <FraudRadarChart 
                    candidateId={candidateId!} 
                    minDate={cycleDateRange.minDate}
                    maxDate={cycleDateRange.maxDate}
                  />
                </Suspense>
              </ErrorBoundary>
              <ErrorBoundary>
                <Suspense fallback={<div className="bg-white rounded-lg shadow p-6"><div className="animate-pulse"><div className="h-64 bg-gray-200 rounded"></div></div></div>}>
                  <SmurfingScatter 
                    candidateId={candidateId} 
                    minDate={cycleDateRange.minDate}
                    maxDate={cycleDateRange.maxDate}
                  />
                </Suspense>
              </ErrorBoundary>
              <ErrorBoundary>
                <Suspense fallback={<div className="bg-white rounded-lg shadow p-6"><div className="animate-pulse"><div className="h-64 bg-gray-200 rounded"></div></div></div>}>
                  <FraudAlerts 
                    candidateId={candidateId!} 
                    minDate={cycleDateRange.minDate}
                    maxDate={cycleDateRange.maxDate}
                  />
                </Suspense>
              </ErrorBoundary>
            </>
          )}
        </div>
      </CandidateContextProvider>
    </div>
  );
}

export default function CandidateDetail() {
  return <CandidateDetailContent />;
}

