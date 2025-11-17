import { BulkDataCycle } from '../services/api';
import { formatDateTime } from '../utils/dateUtils';

interface CycleSelectorProps {
  cycles: BulkDataCycle[];
  selectedCycle: number;
  onCycleChange: (cycle: number) => void;
  onRefresh: () => void;
  onSearchCycles: () => void;
  loading?: boolean;
}

export default function CycleSelector({
  cycles,
  selectedCycle,
  onCycleChange,
  onRefresh,
  onSearchCycles,
  loading = false,
}: CycleSelectorProps) {
  const selectedCycleData = cycles.find((c) => c.cycle === selectedCycle);

  return (
    <div className="bg-white shadow rounded-lg p-6">
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-xl font-semibold text-gray-900">Election Cycle</h2>
      </div>

      <div className="flex gap-2 items-end">
        <div className="flex-1">
          <label htmlFor="cycle-select" className="block text-sm font-medium text-gray-700 mb-2">
            Select Cycle
          </label>
          <select
            id="cycle-select"
            value={selectedCycle}
            onChange={(e) => onCycleChange(parseInt(e.target.value))}
            className="block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm"
            disabled={loading}
          >
            {cycles.length > 0 ? (
              cycles.map((cycleObj) => (
                <option key={cycleObj.cycle} value={cycleObj.cycle}>
                  {cycleObj.cycle}
                  {cycleObj.imported ? ' ✓' : ''}
                  {cycleObj.record_count ? ` (${cycleObj.record_count.toLocaleString()} records)` : ''}
                </option>
              ))
            ) : (
              <option value={2024}>2024</option>
            )}
          </select>
        </div>

        <div className="flex flex-col gap-2">
          <button
            onClick={onRefresh}
            disabled={loading}
            className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed whitespace-nowrap"
          >
            Refresh Status
          </button>
          <button
            onClick={onSearchCycles}
            disabled={loading}
            className="px-4 py-2 text-sm font-medium text-white bg-indigo-600 border border-transparent rounded-md hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed whitespace-nowrap"
            title="Search for additional cycles from FEC API (only needed every couple years)"
          >
            Search for Cycles
          </button>
        </div>
      </div>

      {/* Cycle Metadata */}
      {selectedCycleData && (
        <div className="mt-4 pt-4 border-t border-gray-200">
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-3 text-sm">
            <div>
              <span className="text-gray-500">Status:</span>{' '}
              <span
                className={`font-medium ${
                  selectedCycleData.imported ? 'text-green-600' : 'text-gray-600'
                }`}
              >
                {selectedCycleData.imported ? 'Imported' : 'Not Imported'}
              </span>
            </div>
            {selectedCycleData.record_count !== undefined && (
              <div>
                <span className="text-gray-500">Records:</span>{' '}
                <span className="font-medium text-gray-900">
                  {selectedCycleData.record_count.toLocaleString()}
                </span>
              </div>
            )}
            {selectedCycleData.download_date && (
              <div>
                <span className="text-gray-500">Last Download:</span>{' '}
                <span className="font-medium text-gray-900">
                  {formatDateTime(selectedCycleData.download_date, 'Never')}
                </span>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

