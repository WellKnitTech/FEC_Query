import { BulkDataStatus, BulkDataCycle } from '../services/api';
import { formatDateTime } from '../utils/dateUtils';

interface BulkDataStatusSummaryProps {
  status: BulkDataStatus | null;
  cycles: BulkDataCycle[];
  totalJobs?: number;
  runningJobs?: number;
  completedToday?: number;
}

export default function BulkDataStatusSummary({
  status,
  cycles,
  totalJobs = 0,
  runningJobs = 0,
  completedToday = 0,
}: BulkDataStatusSummaryProps) {
  if (!status) {
    return (
      <div className="bg-white shadow rounded-lg p-6">
        <p className="text-gray-500">No status available</p>
      </div>
    );
  }

  // Calculate top cycles by record count (if available)
  const cyclesWithData = cycles
    .filter((c) => c.imported)
    .sort((a, b) => (b.record_count || 0) - (a.record_count || 0))
    .slice(0, 5);

  // Find most recent cycle
  const mostRecentCycle = cycles.length > 0 ? cycles[0] : null;
  const lastImportTime = mostRecentCycle?.download_date
    ? formatDateTime(mostRecentCycle.download_date, 'Never')
    : 'Never';

  return (
    <div className="bg-white shadow rounded-lg p-6">
      <h2 className="text-xl font-semibold text-gray-900 mb-4">Status Overview</h2>

      {/* Main Stats Grid */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4 mb-6">
        <div className="bg-blue-50 rounded-lg p-4">
          <dt className="text-sm font-medium text-blue-600">Total Records</dt>
          <dd className="mt-1 text-2xl font-semibold text-gray-900">
            {status.total_records?.toLocaleString() || 0}
          </dd>
        </div>

        <div className="bg-green-50 rounded-lg p-4">
          <dt className="text-sm font-medium text-green-600">Available Cycles</dt>
          <dd className="mt-1 text-2xl font-semibold text-gray-900">
            {status.available_cycles?.length || cycles.length || 0}
          </dd>
        </div>

        <div className="bg-purple-50 rounded-lg p-4">
          <dt className="text-sm font-medium text-purple-600">Active Jobs</dt>
          <dd className="mt-1 text-2xl font-semibold text-gray-900">{runningJobs}</dd>
        </div>

        <div className="bg-yellow-50 rounded-lg p-4">
          <dt className="text-sm font-medium text-yellow-600">Completed Today</dt>
          <dd className="mt-1 text-2xl font-semibold text-gray-900">{completedToday}</dd>
        </div>
      </div>

      {/* Additional Details */}
      <dl className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <div>
          <dt className="text-sm font-medium text-gray-500">Bulk Data Enabled</dt>
          <dd className="mt-1">
            {status.enabled ? (
              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                Enabled
              </span>
            ) : (
              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">
                Disabled
              </span>
            )}
          </dd>
        </div>

        <div>
          <dt className="text-sm font-medium text-gray-500">Update Interval</dt>
          <dd className="mt-1 text-sm text-gray-900">{status.update_interval_hours || 24} hours</dd>
        </div>

        <div>
          <dt className="text-sm font-medium text-gray-500">Last Import</dt>
          <dd className="mt-1 text-sm text-gray-900">{lastImportTime}</dd>
        </div>

        <div>
          <dt className="text-sm font-medium text-gray-500">Total Jobs Tracked</dt>
          <dd className="mt-1 text-sm text-gray-900">{totalJobs}</dd>
        </div>
      </dl>

      {/* Top Cycles */}
      {cyclesWithData.length > 0 && (
        <div className="mt-6 pt-6 border-t border-gray-200">
          <h3 className="text-sm font-medium text-gray-700 mb-3">Top Cycles by Record Count</h3>
          <div className="space-y-2">
            {cyclesWithData.map((cycle) => (
              <div key={cycle.cycle} className="flex justify-between items-center text-sm">
                <span className="text-gray-600">Cycle {cycle.cycle}</span>
                <span className="font-medium text-gray-900">
                  {cycle.record_count?.toLocaleString() || 0} records
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

