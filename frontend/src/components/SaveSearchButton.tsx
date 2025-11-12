import { useState } from 'react';
import { savedSearchApi } from '../services/api';

interface SaveSearchButtonProps {
  searchType: 'candidate' | 'committee' | 'race' | 'donor' | 'independent_expenditure';
  searchParams: Record<string, any>;
}

export default function SaveSearchButton({ searchType, searchParams }: SaveSearchButtonProps) {
  const [showDialog, setShowDialog] = useState(false);
  const [name, setName] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  const handleSave = async () => {
    if (!name.trim()) {
      setError('Please enter a name for this search');
      return;
    }

    setSaving(true);
    setError(null);
    setSuccess(false);

    try {
      await savedSearchApi.create(name.trim(), searchType, searchParams);
      setSuccess(true);
      setName('');
      setTimeout(() => {
        setShowDialog(false);
        setSuccess(false);
      }, 1500);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save search');
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="relative inline-block">
      <button
        onClick={() => setShowDialog(true)}
        className="px-3 py-1.5 bg-gray-600 text-white rounded-lg hover:bg-gray-700 text-sm flex items-center gap-1"
      >
        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 5a2 2 0 012-2h10a2 2 0 012 2v16l-7-3.5L5 21V5z" />
        </svg>
        Save Search
      </button>

      {showDialog && (
        <>
          <div
            className="fixed inset-0 bg-black bg-opacity-50 z-40"
            onClick={() => setShowDialog(false)}
          ></div>
          <div className="fixed inset-0 flex items-center justify-center z-50">
            <div className="bg-white rounded-lg shadow-xl p-6 max-w-md w-full mx-4">
              <h3 className="text-lg font-semibold mb-4">Save Search</h3>
              
              <div className="mb-4">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Search Name
                </label>
                <input
                  type="text"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="e.g., Texas Senate Race 2024"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
                  autoFocus
                />
              </div>

              {error && (
                <div className="mb-4 p-3 bg-red-100 border border-red-400 text-red-700 rounded text-sm">
                  {error}
                </div>
              )}

              {success && (
                <div className="mb-4 p-3 bg-green-100 border border-green-400 text-green-700 rounded text-sm">
                  Search saved successfully!
                </div>
              )}

              <div className="flex justify-end gap-2">
                <button
                  onClick={() => {
                    setShowDialog(false);
                    setName('');
                    setError(null);
                    setSuccess(false);
                  }}
                  className="px-4 py-2 text-gray-700 border border-gray-300 rounded-lg hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  onClick={handleSave}
                  disabled={saving || !name.trim()}
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
                >
                  {saving ? 'Saving...' : 'Save'}
                </button>
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

