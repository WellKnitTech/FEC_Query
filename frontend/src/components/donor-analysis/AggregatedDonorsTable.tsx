import { AggregatedDonor } from '../../services/api';
import { formatDate } from '../../utils/dateUtils';
import PaginationControls from './PaginationControls';

interface AggregatedDonorsTableProps {
  aggregatedDonors: AggregatedDonor[];
  currentPage: number;
  itemsPerPage: number;
  sortColumn: string | null;
  onSort: (column: string) => void;
  onPageChange: (page: number) => void;
  onItemsPerPageChange: (itemsPerPage: number) => void;
  getSortIcon: (column: string) => string;
}

export default function AggregatedDonorsTable({
  aggregatedDonors,
  currentPage,
  itemsPerPage,
  sortColumn,
  onSort,
  onPageChange,
  onItemsPerPageChange,
  getSortIcon,
}: AggregatedDonorsTableProps) {
  if (aggregatedDonors.length === 0) return null;

  const totalPages = Math.ceil(aggregatedDonors.length / itemsPerPage);
  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  const paginatedDonors = aggregatedDonors.slice(startIndex, endIndex);

  return (
    <div className="bg-white rounded-lg shadow">
      <div className="px-6 py-4 border-b border-gray-200 flex justify-between items-center">
        <h2 className="text-lg font-semibold">Aggregated Donors ({aggregatedDonors.length})</h2>
        <div className="flex items-center gap-2">
          <label className="text-sm text-gray-600">Show:</label>
          <select
            value={itemsPerPage}
            onChange={(e) => {
              onItemsPerPageChange(Number(e.target.value));
              onPageChange(1);
            }}
            className="px-3 py-1 border border-gray-300 rounded-lg text-sm"
          >
            <option value={10}>10</option>
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
          </select>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => onSort('name')}
              >
                <div className="flex items-center gap-1">
                  Donor Name
                  <span className={sortColumn === 'name' ? 'text-blue-600' : 'text-gray-400'}>
                    {getSortIcon('name')}
                  </span>
                </div>
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => onSort('amount')}
              >
                <div className="flex items-center gap-1">
                  Total Amount
                  <span className="text-blue-600">{getSortIcon('amount')}</span>
                </div>
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => onSort('count')}
              >
                <div className="flex items-center gap-1">
                  Contributions
                  <span className="text-blue-600">{getSortIcon('count')}</span>
                </div>
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => onSort('state')}
              >
                <div className="flex items-center gap-1">
                  State
                  <span className="text-blue-600">{getSortIcon('state')}</span>
                </div>
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => onSort('employer')}
              >
                <div className="flex items-center gap-1">
                  Employer
                  <span className="text-blue-600">{getSortIcon('employer')}</span>
                </div>
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Date Range
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => onSort('confidence')}
              >
                <div className="flex items-center gap-1">
                  Match Confidence
                  <span className="text-blue-600">{getSortIcon('confidence')}</span>
                </div>
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {paginatedDonors.map((donor, idx) => (
              <tr key={donor.donor_key || idx} className="hover:bg-gray-50">
                <td className="px-6 py-4 whitespace-nowrap text-sm">
                  <div className="font-medium text-gray-900">{donor.canonical_name}</div>
                  {donor.all_names.length > 1 && (
                    <div className="text-xs text-gray-500 mt-1" title={donor.all_names.join(', ')}>
                      {donor.all_names.length} name variation{donor.all_names.length !== 1 ? 's' : ''}
                      {donor.all_names.length <= 3 && (
                        <span className="ml-1">({donor.all_names.slice(0, 3).join(', ')})</span>
                      )}
                    </div>
                  )}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                  ${donor.total_amount.toLocaleString()}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {donor.contribution_count}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {donor.canonical_state || 'N/A'}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {donor.canonical_employer || 'N/A'}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {donor.first_contribution_date && donor.last_contribution_date ? (
                    <div>
                      <div>{formatDate(donor.first_contribution_date)}</div>
                      <div className="text-xs text-gray-400">to</div>
                      <div>{formatDate(donor.last_contribution_date)}</div>
                    </div>
                  ) : (
                    'N/A'
                  )}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  <div className="flex items-center gap-2">
                    <span>{(donor.match_confidence * 100).toFixed(0)}%</span>
                    <div className="w-16 h-2 bg-gray-200 rounded-full overflow-hidden">
                      <div
                        className={`h-full ${
                          donor.match_confidence >= 0.8
                            ? 'bg-green-500'
                            : donor.match_confidence >= 0.6
                            ? 'bg-yellow-500'
                            : 'bg-red-500'
                        }`}
                        style={{ width: `${donor.match_confidence * 100}%` }}
                      />
                    </div>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <PaginationControls
        currentPage={currentPage}
        totalPages={totalPages}
        itemsPerPage={itemsPerPage}
        totalItems={aggregatedDonors.length}
        onPageChange={onPageChange}
      />
    </div>
  );
}

