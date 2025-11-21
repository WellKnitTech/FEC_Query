import { useState, useMemo, useCallback } from 'react';
import { useDonorAnalysis } from '../hooks/useDonorAnalysis';
import { useFilters } from '../hooks/useFilters';
import { usePagination } from '../hooks/usePagination';
import { useEntityNames } from '../hooks/useEntityNames';
import { exportApi } from '../services/api';
import {
  processChartData,
  processContributionsByState,
  processTopCandidates,
  processTopCommittees,
  processContributionFrequency,
  processAmountDistribution,
  sortContributions,
  sortAggregatedDonors,
} from '../utils/donorAnalysisUtils';
import {
  SearchForm,
  DonorList,
  SummaryCards,
  ChartsSection,
  TopEntities,
  ContributionsTable,
  AggregatedDonorsTable,
} from '../components/donor-analysis';

export default function DonorAnalysis() {
  const [searchTerm, setSearchTerm] = useState('');
  const [showEmployer, setShowEmployer] = useState(false);
  const [showOccupation, setShowOccupation] = useState(false);

  // Simplified hook
  const donorAnalysis = useDonorAnalysis();
  const filters = useFilters();
  const pagination = usePagination();

  // Process contributions data
  const topCandidates = useMemo(
    () => processTopCandidates(donorAnalysis.contributions),
    [donorAnalysis.contributions]
  );
  const topCommittees = useMemo(
    () => processTopCommittees(donorAnalysis.contributions),
    [donorAnalysis.contributions]
  );

  const entityNames = useEntityNames(
    donorAnalysis.contributions,
    topCandidates,
    topCommittees
  );

  // Calculate derived data
  const chartData = useMemo(
    () => processChartData(donorAnalysis.contributions),
    [donorAnalysis.contributions]
  );

  const contributionsByState = useMemo(
    () => processContributionsByState(donorAnalysis.contributions),
    [donorAnalysis.contributions]
  );

  const contributionFrequency = useMemo(
    () => processContributionFrequency(donorAnalysis.contributions),
    [donorAnalysis.contributions]
  );

  const amountDistribution = useMemo(
    () => processAmountDistribution(donorAnalysis.contributions),
    [donorAnalysis.contributions]
  );

  const stats = useMemo(
    () =>
      donorAnalysis.contributions.length > 0
        ? {
            totalAmount: donorAnalysis.contributions.reduce(
              (sum, c) => sum + (c.contribution_amount || 0),
              0
            ),
            uniqueCommittees: new Set(
              donorAnalysis.contributions
                .map((c) => c.committee_id)
                .filter(Boolean)
            ).size,
            averageContribution:
              donorAnalysis.contributions.length > 0
                ? donorAnalysis.contributions.reduce(
                    (sum, c) => sum + (c.contribution_amount || 0),
                    0
                  ) / donorAnalysis.contributions.length
                : 0,
          }
        : {
            totalAmount: 0,
            uniqueCommittees: 0,
            averageContribution: 0,
          },
    [donorAnalysis.contributions]
  );

  // Sort contributions
  const sortedContributions = useMemo(
    () =>
      sortContributions(
        donorAnalysis.contributions,
        pagination.sortColumn,
        pagination.sortDirection,
        entityNames.candidateNames,
        entityNames.committeeNames
      ),
    [
      donorAnalysis.contributions,
      pagination.sortColumn,
      pagination.sortDirection,
      entityNames.candidateNames,
      entityNames.committeeNames,
    ]
  );

  const sortedAggregatedDonors = useMemo(
    () =>
      sortAggregatedDonors(
        donorAnalysis.aggregatedDonors,
        pagination.sortColumn,
        pagination.sortDirection
      ),
    [
      donorAnalysis.aggregatedDonors,
      pagination.sortColumn,
      pagination.sortDirection,
    ]
  );

  // Handlers
  const handleSearch = useCallback(
    async (e: React.FormEvent) => {
      e.preventDefault();
      if (!searchTerm.trim()) return;
      await donorAnalysis.search(searchTerm.trim());
    },
    [searchTerm, donorAnalysis]
  );

  const handleSelectDonor = useCallback(
    async (donorName: string) => {
      await donorAnalysis.selectDonor(donorName);
      await donorAnalysis.loadContributions({
        minDate: filters.filters.minDate || undefined,
        maxDate: filters.filters.maxDate || undefined,
        minAmount: filters.filters.minAmount,
        maxAmount: filters.filters.maxAmount,
      });
    },
    [donorAnalysis, filters.filters]
  );

  const handleToggleView = useCallback(async () => {
    await donorAnalysis.toggleView();
    await donorAnalysis.loadContributions({
      minDate: filters.filters.minDate || undefined,
      maxDate: filters.filters.maxDate || undefined,
      minAmount: filters.filters.minAmount,
      maxAmount: filters.filters.maxAmount,
    });
  }, [donorAnalysis, filters.filters]);

  const handleClear = useCallback(() => {
    donorAnalysis.clear();
    filters.clearFilters();
    pagination.resetPagination();
    setSearchTerm('');
    setShowEmployer(false);
    setShowOccupation(false);
  }, [donorAnalysis, filters, pagination]);

  const handleExport = useCallback(
    async (format: 'csv' | 'excel') => {
      try {
        const nameToExport = donorAnalysis.selectedDonor || searchTerm;
        await exportApi.exportContributions(format, {
          contributor_name: nameToExport,
          min_date: filters.filters.minDate || undefined,
          max_date: filters.filters.maxDate || undefined,
          min_amount: filters.filters.minAmount,
          max_amount: filters.filters.maxAmount,
        });
      } catch (err: any) {
        console.error('Export error:', err);
      }
    },
    [donorAnalysis.selectedDonor, searchTerm, filters.filters]
  );

  const handleBackToDonors = useCallback(() => {
    donorAnalysis.clear();
    setSearchTerm('');
  }, [donorAnalysis]);

  const hasData =
    (donorAnalysis.viewAggregated && donorAnalysis.aggregatedDonors.length > 0) ||
    (!donorAnalysis.viewAggregated && donorAnalysis.contributions.length > 0);

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <h1 className="text-3xl font-bold text-gray-900 mb-6">Donor Analysis</h1>

      <SearchForm
        contributorName={searchTerm}
        setContributorName={setSearchTerm}
        onSubmit={handleSearch}
        loading={donorAnalysis.loading}
        onCancel={() => {}}
        onClear={handleClear}
        onExport={handleExport}
        hasContributions={hasData}
        filters={filters.filters}
        setFilters={filters.setFilters}
        handleDatePreset={filters.handleDatePreset}
        handleAmountPreset={filters.handleAmountPreset}
        showCancel={false}
      />

      {donorAnalysis.error && (
        <div className="mb-4 p-4 bg-red-100 border border-red-400 text-red-700 rounded">
          <div className="flex items-center justify-between">
            <span>{donorAnalysis.error}</span>
            {searchTerm.trim() && (
              <button
                onClick={() => donorAnalysis.search(searchTerm.trim())}
                className="ml-4 px-3 py-1 bg-red-600 text-white rounded hover:bg-red-700 text-sm"
              >
                Retry
              </button>
            )}
          </div>
        </div>
      )}

      {donorAnalysis.loading && (
        <div className="mb-4 p-4 bg-blue-50 border border-blue-200 text-blue-800 rounded">
          <div className="flex items-center gap-2">
            <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-600"></div>
            <span>Loading...</span>
          </div>
        </div>
      )}

      {donorAnalysis.donors.length > 0 && !donorAnalysis.selectedDonor && (
        <DonorList
          contributors={donorAnalysis.donors}
          onSelect={handleSelectDonor}
          searchTerm={searchTerm}
        />
      )}

      {donorAnalysis.selectedDonor && (
        <div className="mb-4 p-4 bg-blue-50 border border-blue-200 rounded">
          <div className="flex justify-between items-center">
            <span className="text-blue-800">
              Showing contributions for: <strong>{donorAnalysis.selectedDonor}</strong>
            </span>
            <button
              onClick={handleBackToDonors}
              className="text-blue-600 hover:text-blue-800 text-sm underline"
            >
              Change contributor
            </button>
          </div>
        </div>
      )}

      {hasData && (
        <div className="space-y-6">
          <div className="bg-white rounded-lg shadow p-4">
            <div className="flex items-center gap-4">
              <label className="flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={donorAnalysis.viewAggregated}
                  onChange={handleToggleView}
                  disabled={donorAnalysis.loading}
                  className="rounded"
                />
                <span className="text-sm font-medium">View Aggregated Donors</span>
              </label>
              <span className="text-xs text-gray-500">
                {donorAnalysis.viewAggregated
                  ? 'Showing unique donors (grouped by name variations)'
                  : 'Showing individual contributions'}
              </span>
            </div>
          </div>

          <SummaryCards
            contributions={donorAnalysis.contributions}
            aggregatedDonors={donorAnalysis.aggregatedDonors}
            viewAggregated={donorAnalysis.viewAggregated}
          />

          {!donorAnalysis.viewAggregated && (
            <>
              {chartData && (
                <ChartsSection
                  contributions={donorAnalysis.contributions}
                  contributionsByState={contributionsByState}
                  chartData={chartData}
                  amountDistribution={amountDistribution}
                  contributionFrequency={contributionFrequency}
                  averageContribution={stats.averageContribution}
                  uniqueCommittees={stats.uniqueCommittees}
                  totalAmount={stats.totalAmount}
                  viewAggregated={donorAnalysis.viewAggregated}
                />
              )}

              {topCandidates.length > 0 || topCommittees.length > 0 ? (
                <TopEntities
                  topCandidates={topCandidates}
                  topCommittees={topCommittees}
                  candidateNames={entityNames.candidateNames}
                  committeeNames={entityNames.committeeNames}
                />
              ) : null}
            </>
          )}

          {donorAnalysis.viewAggregated ? (
            <AggregatedDonorsTable
              aggregatedDonors={sortedAggregatedDonors}
              currentPage={pagination.currentPage}
              itemsPerPage={pagination.itemsPerPage}
              sortColumn={pagination.sortColumn}
              onSort={pagination.handleSort}
              onPageChange={pagination.setPage}
              onItemsPerPageChange={pagination.setItemsPerPage}
              getSortIcon={pagination.getSortIcon}
            />
          ) : (
            <ContributionsTable
              contributions={sortedContributions}
              candidateNames={entityNames.candidateNames}
              committeeNames={entityNames.committeeNames}
              currentPage={pagination.currentPage}
              itemsPerPage={pagination.itemsPerPage}
              sortColumn={pagination.sortColumn}
              onSort={pagination.handleSort}
              onPageChange={pagination.setPage}
              onItemsPerPageChange={pagination.setItemsPerPage}
              getSortIcon={pagination.getSortIcon}
              showEmployer={showEmployer}
              showOccupation={showOccupation}
              onShowEmployerChange={setShowEmployer}
              onShowOccupationChange={setShowOccupation}
            />
          )}
        </div>
      )}
    </div>
  );
}
