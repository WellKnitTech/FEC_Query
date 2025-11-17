import { BulkDataStatus, BulkDataCycle } from '../services/api';
import { formatDateTime } from '../utils/dateUtils';

interface QuickStatsProps {
  status: BulkDataStatus | null;
  cycles: BulkDataCycle[];
  runningJobs: number;
}

export default function QuickStats({ status, cycles, runningJobs }: QuickStatsProps) {
  // Find most recent import
  const mostRecentCycle = cycles
    .filter((c) => c.download_date)
    .sort((a, b) => {
      const aTime = a.download_date ? new Date(a.download_date).getTime() : 0;
      const bTime = b.download_date ? new Date(b.download_date).getTime() : 0;
      return bTime - aTime;
    })[0];

  const lastImport = mostRecentCycle?.download_date
    ? formatDateTime(mostRecentCycle.download_date, 'Never')
    : 'Never';

  // Count cycles with complete data
  const cyclesWithData = cycles.filter((c) => c.imported && (c.record_count || 0) > 0).length;

  return (
    <div className="bg-gradient-to-r from-blue-50 to-indigo-50 border border-blue-200 rounded-lg p-4">
      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <div className="text-center">
          <div className="text-2xl font-bold text-gray-900">
            {status?.total_records?.toLocaleString() || 0}
          </div>
          <div className="text-xs text-gray-600 mt-1">Total Records</div>
        </div>

        <div className="text-center">
          <div className="text-2xl font-bold text-gray-900">{cyclesWithData}</div>
          <div className="text-xs text-gray-600 mt-1">Cycles with Data</div>
        </div>

        <div className="text-center">
          <div className="text-2xl font-bold text-gray-900">{runningJobs}</div>
          <div className="text-xs text-gray-600 mt-1">Active Jobs</div>
        </div>

        <div className="text-center">
          <div className="text-sm font-medium text-gray-900">{lastImport}</div>
          <div className="text-xs text-gray-600 mt-1">Last Import</div>
        </div>
      </div>
    </div>
  );
}

