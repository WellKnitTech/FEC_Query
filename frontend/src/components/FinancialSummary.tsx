import { useFinancialData } from '../hooks/useFinancialData';
import { useCycleSelector } from '../hooks/useCycleSelector';
import AnalysisSection from './candidate/AnalysisSection';
import { formatCurrency } from '../utils/candidateCalculations';

interface FinancialSummaryProps {
  candidateId: string;
  cycle?: number;
  onCycleChange?: (cycle: number | undefined) => void;
}

export default function FinancialSummaryComponent({ candidateId, cycle, onCycleChange }: FinancialSummaryProps) {
  const { financials, selected, latest, loading, error, availableCycles, refresh } = useFinancialData(
    candidateId,
    cycle
  );

  const { selectedCycle, setCycle, hasMultipleCycles } = useCycleSelector({
    financials,
    initialCycle: cycle,
    onCycleChange,
  });

  // Use selected financial if cycle is specified, otherwise use latest
  const displayFinancial = selected || latest;

  if (!displayFinancial && !loading && !error) {
    return (
      <AnalysisSection title="Financial Summary" loading={false} error={null}>
        <p className="text-gray-600">No financial data available</p>
      </AnalysisSection>
    );
  }

  return (
    <AnalysisSection
      title="Financial Summary"
      loading={loading}
      error={error}
      onRetry={refresh}
    >
      <div className="mb-4">
        {hasMultipleCycles && (
          <div className="flex items-center gap-2 mb-4">
            <label htmlFor="cycle-select" className="text-sm font-medium text-gray-700">
              Cycle:
            </label>
            <select
              id="cycle-select"
              value={selectedCycle ?? ''}
              onChange={(e) => {
                const newCycle = e.target.value ? parseInt(e.target.value, 10) : undefined;
                setCycle(newCycle);
              }}
              className="px-3 py-1 border border-gray-300 rounded-md text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            >
              <option value="">Latest ({latest?.cycle ?? 'N/A'})</option>
              {availableCycles.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
            {displayFinancial?.cycle && (
              <span className="text-sm text-gray-500">
                (Currently showing: Cycle {displayFinancial.cycle})
              </span>
            )}
          </div>
        )}
        {displayFinancial?.cycle !== undefined && !hasMultipleCycles && (
          <div className="text-sm text-gray-500 mb-4">Cycle {displayFinancial.cycle}</div>
        )}
      </div>

      {displayFinancial && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div className="bg-blue-50 p-4 rounded-lg">
              <div className="text-sm text-gray-600 mb-1">Total Receipts</div>
              <div className="text-2xl font-bold text-blue-900">
                {formatCurrency(displayFinancial.total_receipts)}
              </div>
            </div>
            <div className="bg-green-50 p-4 rounded-lg">
              <div className="text-sm text-gray-600 mb-1">Cash on Hand</div>
              <div className="text-2xl font-bold text-green-900">
                {formatCurrency(displayFinancial.cash_on_hand)}
              </div>
            </div>
            <div className="bg-red-50 p-4 rounded-lg">
              <div className="text-sm text-gray-600 mb-1">Total Disbursements</div>
              <div className="text-2xl font-bold text-red-900">
                {formatCurrency(displayFinancial.total_disbursements)}
              </div>
            </div>
            <div className="bg-purple-50 p-4 rounded-lg">
              <div className="text-sm text-gray-600 mb-1">Total Contributions</div>
              <div className="text-2xl font-bold text-purple-900">
                {formatCurrency(displayFinancial.total_contributions)}
              </div>
            </div>
          </div>
          <div className="mt-6 grid grid-cols-2 md:grid-cols-4 gap-4">
            <div>
              <div className="text-sm text-gray-600">Individual Contributions</div>
              <div className="text-lg font-semibold">
                {formatCurrency(displayFinancial.individual_contributions)}
              </div>
            </div>
            <div>
              <div className="text-sm text-gray-600">PAC Contributions</div>
              <div className="text-lg font-semibold">
                {formatCurrency(displayFinancial.pac_contributions)}
              </div>
            </div>
            <div>
              <div className="text-sm text-gray-600">Party Contributions</div>
              <div className="text-lg font-semibold">
                {formatCurrency(displayFinancial.party_contributions)}
              </div>
            </div>
            <div>
              <div className="text-sm text-gray-600">Loans Received</div>
              <div className="text-lg font-semibold">
                {formatCurrency(displayFinancial.loan_contributions || 0)}
              </div>
            </div>
          </div>
        </>
      )}
    </AnalysisSection>
  );
}

