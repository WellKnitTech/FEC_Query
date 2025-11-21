import { Filters } from '../../hooks/useFilters';

interface SearchFormProps {
  contributorName: string;
  setContributorName: (name: string) => void;
  onSubmit: (e: React.FormEvent) => void;
  loading: boolean;
  onCancel: () => void;
  onClear: () => void;
  onExport: (format: 'csv' | 'excel') => void;
  hasContributions: boolean;
  filters: Filters;
  setFilters: (filters: Partial<Filters>) => void;
  handleDatePreset: (preset: 'last30' | 'lastYear' | 'thisCycle' | 'allTime') => void;
  handleAmountPreset: (preset: '0-100' | '100-500' | '500+' | 'all') => void;
  showCancel: boolean;
}

export default function SearchForm({
  contributorName,
  setContributorName,
  onSubmit,
  loading,
  onCancel,
  onClear,
  onExport,
  hasContributions,
  filters,
  setFilters,
  handleDatePreset,
  handleAmountPreset,
  showCancel,
}: SearchFormProps) {
  return (
    <form onSubmit={onSubmit} className="mb-6">
      <div className="space-y-4">
        <div className="flex gap-2">
          <input
            type="text"
            value={contributorName}
            onChange={(e) => setContributorName(e.target.value)}
            placeholder="Enter contributor name..."
            className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
          <button
            type="submit"
            disabled={loading}
            className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
          >
            {loading ? 'Searching...' : 'Search'}
          </button>
          {showCancel && (
            <button
              type="button"
              onClick={onCancel}
              className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700"
            >
              Cancel
            </button>
          )}
          <button
            type="button"
            onClick={onClear}
            className="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300"
          >
            Clear Filters
          </button>
          {hasContributions && (
            <>
              <button
                type="button"
                onClick={() => onExport('csv')}
                className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700"
              >
                Export CSV
              </button>
              <button
                type="button"
                onClick={() => onExport('excel')}
                className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700"
              >
                Export Excel
              </button>
            </>
          )}
        </div>

        {/* Date Range Filters */}
        <div className="flex flex-wrap gap-4 items-end">
          <div className="flex-1 min-w-[200px]">
            <label className="block text-sm font-medium text-gray-700 mb-1">Date Range</label>
            <div className="flex gap-2">
              <input
                type="date"
                value={filters.minDate}
                onChange={(e) => setFilters({ minDate: e.target.value })}
                className="flex-1 px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                placeholder="Start date"
              />
              <span className="self-center text-gray-500">to</span>
              <input
                type="date"
                value={filters.maxDate}
                onChange={(e) => setFilters({ maxDate: e.target.value })}
                className="flex-1 px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                placeholder="End date"
              />
            </div>
            <div className="flex gap-2 mt-2">
              <button
                type="button"
                onClick={() => handleDatePreset('last30')}
                className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
              >
                Last 30 days
              </button>
              <button
                type="button"
                onClick={() => handleDatePreset('lastYear')}
                className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
              >
                Last year
              </button>
              <button
                type="button"
                onClick={() => handleDatePreset('thisCycle')}
                className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
              >
                This cycle
              </button>
              <button
                type="button"
                onClick={() => handleDatePreset('allTime')}
                className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
              >
                All time
              </button>
            </div>
          </div>

          {/* Amount Range Filters */}
          <div className="flex-1 min-w-[200px]">
            <label className="block text-sm font-medium text-gray-700 mb-1">Amount Range</label>
            <div className="flex gap-2">
              <input
                type="number"
                value={filters.minAmount || ''}
                onChange={(e) =>
                  setFilters({ minAmount: e.target.value ? parseFloat(e.target.value) : undefined })
                }
                placeholder="Min"
                min="0"
                step="0.01"
                className="flex-1 px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
              <span className="self-center text-gray-500">to</span>
              <input
                type="number"
                value={filters.maxAmount || ''}
                onChange={(e) =>
                  setFilters({ maxAmount: e.target.value ? parseFloat(e.target.value) : undefined })
                }
                placeholder="Max"
                min="0"
                step="0.01"
                className="flex-1 px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
            <div className="flex gap-2 mt-2">
              <button
                type="button"
                onClick={() => handleAmountPreset('0-100')}
                className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
              >
                $0-$100
              </button>
              <button
                type="button"
                onClick={() => handleAmountPreset('100-500')}
                className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
              >
                $100-$500
              </button>
              <button
                type="button"
                onClick={() => handleAmountPreset('500+')}
                className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
              >
                $500+
              </button>
              <button
                type="button"
                onClick={() => handleAmountPreset('all')}
                className="px-3 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200"
              >
                All amounts
              </button>
            </div>
          </div>
        </div>
      </div>
    </form>
  );
}

