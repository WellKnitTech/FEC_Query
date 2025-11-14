import { useState, useEffect } from 'react';
import { settingsApi, ApiKeyStatus } from '../services/api';

export default function Settings() {
  const [apiKeyStatus, setApiKeyStatus] = useState<ApiKeyStatus | null>(null);
  const [newApiKey, setNewApiKey] = useState('');
  const [showKey, setShowKey] = useState(false);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  useEffect(() => {
    loadApiKeyStatus();
  }, []);

  const loadApiKeyStatus = async () => {
    setLoading(true);
    setError(null);
    try {
      const status = await settingsApi.getApiKey();
      setApiKeyStatus(status);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to load API key status');
    } finally {
      setLoading(false);
    }
  };

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newApiKey.trim()) {
      setError('Please enter an API key');
      return;
    }

    setSaving(true);
    setError(null);
    setSuccess(null);
    try {
      await settingsApi.setApiKey(newApiKey.trim());
      setSuccess('API key saved successfully!');
      setNewApiKey('');
      await loadApiKeyStatus();
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save API key');
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!confirm('Are you sure you want to remove the API key? The system will fall back to the environment variable.')) {
      return;
    }

    setDeleting(true);
    setError(null);
    setSuccess(null);
    try {
      await settingsApi.deleteApiKey();
      setSuccess('API key removed successfully!');
      await loadApiKeyStatus();
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to remove API key');
    } finally {
      setDeleting(false);
    }
  };

  if (loading) {
    return (
      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="bg-white rounded-lg shadow p-6">
          <div className="text-center">Loading...</div>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">Settings</h1>
        <p className="text-gray-600">
          Configure your FEC API key. Keys set via the UI take precedence over environment variables.
        </p>
      </div>

      {/* API Key Section */}
      <div className="bg-white rounded-lg shadow">
        <div className="px-6 py-4 border-b border-gray-200">
          <h2 className="text-xl font-semibold">FEC API Key Configuration</h2>
        </div>
        <div className="px-6 py-6">
          {/* Current Status */}
          {apiKeyStatus && (
            <div className="mb-6">
              <div className="flex items-center justify-between mb-4">
                <div>
                  <h3 className="text-lg font-medium text-gray-900">Current API Key</h3>
                  <p className="text-sm text-gray-600 mt-1">
                    {apiKeyStatus.has_key ? (
                      <>
                        Source: <span className="font-semibold">{apiKeyStatus.source === 'ui' ? 'UI Configuration' : 'Environment Variable'}</span>
                      </>
                    ) : (
                      'No API key configured'
                    )}
                  </p>
                </div>
                {apiKeyStatus.has_key && apiKeyStatus.key_preview && (
                  <div className="flex items-center gap-2">
                    <code className="px-3 py-1 bg-gray-100 rounded text-sm font-mono">
                      {showKey ? apiKeyStatus.key_preview : '••••••••'}
                    </code>
                    <button
                      type="button"
                      onClick={() => setShowKey(!showKey)}
                      className="text-sm text-blue-600 hover:text-blue-800"
                    >
                      {showKey ? 'Hide' : 'Show'}
                    </button>
                  </div>
                )}
              </div>

              {apiKeyStatus.has_key && apiKeyStatus.source === 'ui' && (
                <button
                  type="button"
                  onClick={handleDelete}
                  disabled={deleting}
                  className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {deleting ? 'Removing...' : 'Remove API Key'}
                </button>
              )}
            </div>
          )}

          {/* Error/Success Messages */}
          {error && (
            <div className="mb-4 p-4 bg-red-50 border border-red-200 rounded-lg">
              <p className="text-sm text-red-800">{error}</p>
            </div>
          )}

          {success && (
            <div className="mb-4 p-4 bg-green-50 border border-green-200 rounded-lg">
              <p className="text-sm text-green-800">{success}</p>
            </div>
          )}

          {/* Set/Update API Key Form */}
          <form onSubmit={handleSave} className="space-y-4">
            <div>
              <label htmlFor="api-key" className="block text-sm font-medium text-gray-700 mb-2">
                {apiKeyStatus?.has_key && apiKeyStatus.source === 'ui' ? 'Update API Key' : 'Set API Key'}
              </label>
              <input
                type="password"
                id="api-key"
                value={newApiKey}
                onChange={(e) => setNewApiKey(e.target.value)}
                placeholder="Enter your FEC API key"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                disabled={saving}
              />
              <p className="mt-2 text-sm text-gray-500">
                Get your API key from{' '}
                <a
                  href="https://api.open.fec.gov/developers/"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-blue-600 hover:text-blue-800"
                >
                  FEC API Developer Portal
                </a>
              </p>
            </div>
            <button
              type="submit"
              disabled={saving || !newApiKey.trim()}
              className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {saving ? 'Saving...' : apiKeyStatus?.has_key && apiKeyStatus.source === 'ui' ? 'Update Key' : 'Save Key'}
            </button>
          </form>

          {/* Info Box */}
          <div className="mt-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
            <h4 className="text-sm font-semibold text-blue-900 mb-2">About API Keys</h4>
            <ul className="text-sm text-blue-800 space-y-1 list-disc list-inside">
              <li>API keys set via the UI take precedence over environment variables</li>
              <li>Keys are stored securely in the database</li>
              <li>You can update or remove your key at any time</li>
              <li>If no UI key is set, the system will use the environment variable (if configured)</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

