import { CycleStatus } from '../services/api';
import DataTypeStatusBadge from './DataTypeStatusBadge';

interface DataTypeGridProps {
  cycleStatus: CycleStatus | null;
  selectedTypes: Set<string>;
  onSelectionChange: (dataType: string, selected: boolean) => void;
  onSelectAll: () => void;
  onDeselectAll: () => void;
  loading?: boolean;
}

export default function DataTypeGrid({
  cycleStatus,
  selectedTypes,
  onSelectionChange,
  onSelectAll,
  onDeselectAll,
  loading = false,
}: DataTypeGridProps) {
  if (!cycleStatus) {
    return (
      <div className="bg-white shadow rounded-lg p-6">
        <div className="text-center text-gray-500">Loading data types...</div>
      </div>
    );
  }

  // Safety check: ensure data_types exists and is an array
  if (!cycleStatus.data_types || !Array.isArray(cycleStatus.data_types)) {
    return (
      <div className="bg-white shadow rounded-lg p-6">
        <div className="text-center text-red-600">
          Error: Invalid data structure received. Please refresh the page.
        </div>
      </div>
    );
  }

  const implementedTypes = cycleStatus.data_types.filter((dt) => dt.is_implemented);
  const allImplementedSelected = implementedTypes.length > 0 && implementedTypes.every((dt) => selectedTypes.has(dt.data_type));

  const formatDate = (dateStr: string | null | undefined) => {
    if (!dateStr) return 'N/A';
    try {
      return new Date(dateStr).toLocaleDateString();
    } catch {
      return 'N/A';
    }
  };

  const formatNumber = (num: number) => {
    return num.toLocaleString();
  };

  return (
    <div className="bg-white shadow rounded-lg overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-200 flex justify-between items-center">
        <h3 className="text-lg font-semibold text-gray-900">Data Types for {cycleStatus.cycle || 'Unknown'}</h3>
        <div className="flex gap-2">
          <button
            onClick={onSelectAll}
            disabled={loading || implementedTypes.length === 0}
            className="text-sm text-blue-600 hover:text-blue-800 disabled:text-gray-400 disabled:cursor-not-allowed"
          >
            Select All Implemented
          </button>
          <span className="text-gray-300">|</span>
          <button
            onClick={onDeselectAll}
            disabled={loading}
            className="text-sm text-blue-600 hover:text-blue-800 disabled:text-gray-400 disabled:cursor-not-allowed"
          >
            Deselect All
          </button>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider w-12">
                <input
                  type="checkbox"
                  checked={allImplementedSelected}
                  onChange={(e) => (e.target.checked ? onSelectAll() : onDeselectAll())}
                  disabled={loading || implementedTypes.length === 0}
                  className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                />
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Data Type
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Description
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Status
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Records
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Last Downloaded
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Last Imported
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {cycleStatus.data_types.map((dataType) => {
              // Safety check: ensure dataType has required properties
              if (!dataType || !dataType.data_type) {
                return null;
              }
              
              return (
                <tr key={dataType.data_type} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <input
                      type="checkbox"
                      checked={selectedTypes.has(dataType.data_type)}
                      onChange={(e) => onSelectionChange(dataType.data_type, e.target.checked)}
                      disabled={loading || !dataType.is_implemented}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded disabled:opacity-50 disabled:cursor-not-allowed"
                    />
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="text-sm font-medium text-gray-900">{dataType.data_type.replace(/_/g, ' ')}</div>
                    {!dataType.is_implemented && (
                      <div className="text-xs text-yellow-600 mt-1">Parser not implemented yet</div>
                    )}
                  </td>
                  <td className="px-6 py-4">
                    <div className="text-sm text-gray-500">{dataType.description || 'N/A'}</div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <DataTypeStatusBadge status={dataType} />
                    {dataType.error_message && (
                      <div className="text-xs text-red-600 mt-1" title={dataType.error_message}>
                        {dataType.error_message.substring(0, 50)}...
                      </div>
                    )}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="text-sm text-gray-900">{formatNumber(dataType.record_count || 0)}</div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="text-sm text-gray-500">{formatDate(dataType.download_date)}</div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="text-sm text-gray-500">{formatDate(dataType.last_imported_at)}</div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

