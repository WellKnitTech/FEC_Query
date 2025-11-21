import { useState } from 'react';
import { exportApi } from '../services/api';

interface ExportButtonProps {
  candidateId?: string;
  candidateIds?: string[];
  office?: string;
  state?: string;
  district?: string;
  year?: number;
  cycle?: number;
  isRace?: boolean;
}

export default function ExportButton({
  candidateId,
  candidateIds,
  office,
  state,
  district,
  year,
  cycle,
  isRace = false,
}: ExportButtonProps) {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showMenu, setShowMenu] = useState(false);

  const handleExport = async (format: 'pdf' | 'docx' | 'md' | 'csv' | 'excel') => {
    setLoading(true);
    setError(null);
    setShowMenu(false);

    try {
      if (isRace && candidateIds && office && state) {
        await exportApi.exportRace(candidateIds, office, state, format, district, year);
      } else if (candidateId) {
        await exportApi.exportCandidate(candidateId, format, cycle);
      } else {
        throw new Error('Missing required parameters for export');
      }
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to export report');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="relative inline-block">
      <button
        onClick={() => setShowMenu(!showMenu)}
        disabled={loading}
        className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
      >
        {loading ? (
          <>
            <svg className="animate-spin h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
            </svg>
            Exporting...
          </>
        ) : (
          <>
            <svg className="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
            </svg>
            Export Report
          </>
        )}
      </button>

      {showMenu && !loading && (
        <>
          <div
            className="fixed inset-0 z-10"
            onClick={() => setShowMenu(false)}
          ></div>
          <div className="absolute right-0 mt-2 w-48 bg-white rounded-md shadow-lg z-20 border border-gray-200">
            <div className="py-1">
              <button
                onClick={() => handleExport('pdf')}
                className="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
              >
                Export as PDF
              </button>
              <button
                onClick={() => handleExport('docx')}
                className="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
              >
                Export as DOCX
              </button>
              <button
                onClick={() => handleExport('md')}
                className="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
              >
                Export as Markdown
              </button>
              <button
                onClick={() => handleExport('csv')}
                className="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
              >
                Export as CSV
              </button>
              <button
                onClick={() => handleExport('excel')}
                className="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100"
              >
                Export as Excel
              </button>
            </div>
          </div>
        </>
      )}

      {error && (
        <div className="mt-2 text-sm text-red-600 bg-red-50 border border-red-200 rounded p-2">
          {error}
        </div>
      )}
    </div>
  );
}

