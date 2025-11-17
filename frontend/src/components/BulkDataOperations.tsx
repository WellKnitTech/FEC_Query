import { useState } from 'react';

interface BulkDataOperationsProps {
  selectedDataTypesCount: number;
  loading: boolean;
  forceDownload: boolean;
  onForceDownloadChange: (checked: boolean) => void;
  onDownload: () => void;
  onImportSelected: () => void;
  onImportAllTypes: () => void;
  onClearContributions: () => void;
  onCleanupAndReimport: () => void;
  onImportAll: () => void;
}

export default function BulkDataOperations({
  selectedDataTypesCount,
  loading,
  forceDownload,
  onForceDownloadChange,
  onDownload,
  onImportSelected,
  onImportAllTypes,
  onClearContributions,
  onCleanupAndReimport,
  onImportAll,
}: BulkDataOperationsProps) {
  const [showHelp, setShowHelp] = useState(false);

  return (
    <div className="bg-white shadow rounded-lg p-6">
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-xl font-semibold text-gray-900">Data Import Operations</h2>
        <button
          onClick={() => setShowHelp(!showHelp)}
          className="text-sm text-blue-600 hover:text-blue-800"
        >
          {showHelp ? 'Hide Help' : 'Show Help'}
        </button>
      </div>

      {/* Help Section */}
      {showHelp && (
        <div className="mb-6 bg-blue-50 border border-blue-200 rounded-lg p-4 text-sm text-blue-800">
          <div className="space-y-2">
            <p>
              <strong>Download & Import:</strong> Downloads and imports FEC bulk CSV data for the
              selected cycle. Uses <strong>smart merge</strong> to update existing records: fixes
              NULL dates, corrects incorrect data, and preserves valid existing information.
            </p>
            <p>
              <strong>Import Selected/All Types:</strong> Import specific data types for the selected
              cycle. Smart merge applies to all imports.
            </p>
            <p>
              <strong>Clear All Contributions:</strong> Removes all contribution records from the
              database. Use this only if you need a completely fresh start.
            </p>
            <p>
              <strong>Cleanup & Reimport:</strong> First clears all contributions, then downloads
              and imports fresh data for the selected cycle. Only needed if you want a completely
              clean import.
            </p>
            <p>
              <strong>Import All Cycles:</strong> Downloads and imports all election cycles. This may
              take several hours to complete.
            </p>
            <p className="mt-2 font-semibold">
              ⚠️ Warning: Clear All Contributions and Cleanup & Reimport are irreversible.
            </p>
          </div>
        </div>
      )}

      {/* Quick Actions */}
      <div className="mb-6">
        <h3 className="text-sm font-medium text-gray-700 mb-3">Quick Actions</h3>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
          <button
            onClick={onDownload}
            disabled={loading}
            className="inline-flex justify-center items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Download & Import
          </button>

          <button
            onClick={onImportSelected}
            disabled={loading || selectedDataTypesCount === 0}
            className="inline-flex justify-center items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Import Selected ({selectedDataTypesCount})
          </button>

          <button
            onClick={onImportAllTypes}
            disabled={loading}
            className="inline-flex justify-center items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Import All Types
          </button>
        </div>
      </div>

      {/* Maintenance Operations */}
      <div className="mb-6">
        <h3 className="text-sm font-medium text-gray-700 mb-3">Maintenance Operations</h3>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
          <button
            onClick={onClearContributions}
            disabled={loading}
            className="inline-flex justify-center items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-yellow-600 hover:bg-yellow-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-yellow-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Clear All Contributions
          </button>

          <button
            onClick={onCleanupAndReimport}
            disabled={loading}
            className="inline-flex justify-center items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-red-600 hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Cleanup & Reimport
          </button>
        </div>
      </div>

      {/* Bulk Operations */}
      <div>
        <h3 className="text-sm font-medium text-gray-700 mb-3">Bulk Operations</h3>
        <div className="grid grid-cols-1 gap-3">
          <button
            onClick={onImportAll}
            disabled={loading}
            className="inline-flex justify-center items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-purple-600 hover:bg-purple-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Import All Cycles
          </button>
        </div>
      </div>

      {/* Force Download Checkbox - Single instance */}
      <div className="mt-6 pt-6 border-t border-gray-200">
        <label className="flex items-center gap-2 text-sm text-gray-700 cursor-pointer">
          <input
            type="checkbox"
            checked={forceDownload}
            onChange={(e) => onForceDownloadChange(e.target.checked)}
            disabled={loading}
            className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded disabled:opacity-50"
          />
          <span>Force download (skip file size check)</span>
        </label>
      </div>
    </div>
  );
}

