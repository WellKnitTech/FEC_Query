import { useNavigate } from 'react-router-dom';
import { Contribution } from '../../services/api';
import PaginationControls from './PaginationControls';

interface ContributionsTableProps {
  contributions: Contribution[];
  candidateNames: Record<string, string>;
  committeeNames: Record<string, string>;
  currentPage: number;
  itemsPerPage: number;
  sortColumn: string | null;
  onSort: (column: string) => void;
  onPageChange: (page: number) => void;
  onItemsPerPageChange: (itemsPerPage: number) => void;
  getSortIcon: (column: string) => string;
  showEmployer: boolean;
  showOccupation: boolean;
  onShowEmployerChange: (show: boolean) => void;
  onShowOccupationChange: (show: boolean) => void;
}

export default function ContributionsTable({
  contributions,
  candidateNames,
  committeeNames,
  currentPage,
  itemsPerPage,
  sortColumn,
  onSort,
  onPageChange,
  onItemsPerPageChange,
  getSortIcon,
  showEmployer,
  showOccupation,
  onShowEmployerChange,
  onShowOccupationChange,
}: ContributionsTableProps) {
  const navigate = useNavigate();

  if (contributions.length === 0) return null;

  const totalPages = Math.ceil(contributions.length / itemsPerPage);
  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  const paginatedContributions = contributions.slice(startIndex, endIndex);

  return (
    <div className="bg-white rounded-lg shadow">
      <div className="px-6 py-4 border-b border-gray-200 flex justify-between items-center">
        <h2 className="text-lg font-semibold">Contributions ({contributions.length})</h2>
        <div className="flex gap-4 items-center">
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
          <div className="flex items-center gap-2">
            <label className="text-sm text-gray-600">Columns:</label>
            <label className="flex items-center gap-1 text-sm">
              <input
                type="checkbox"
                checked={showEmployer}
                onChange={(e) => onShowEmployerChange(e.target.checked)}
                className="rounded"
              />
              Employer
            </label>
            <label className="flex items-center gap-1 text-sm">
              <input
                type="checkbox"
                checked={showOccupation}
                onChange={(e) => onShowOccupationChange(e.target.checked)}
                className="rounded"
              />
              Occupation
            </label>
          </div>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => onSort('date')}
              >
                <div className="flex items-center gap-1">
                  Date
                  <span className={sortColumn === 'date' ? 'text-blue-600' : 'text-gray-400'}>
                    {getSortIcon('date')}
                  </span>
                </div>
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => onSort('amount')}
              >
                <div className="flex items-center gap-1">
                  Amount
                  <span className="text-blue-600">{getSortIcon('amount')}</span>
                </div>
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => onSort('contributor')}
              >
                <div className="flex items-center gap-1">
                  Contributor
                  <span className="text-blue-600">{getSortIcon('contributor')}</span>
                </div>
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => onSort('candidate')}
              >
                <div className="flex items-center gap-1">
                  Candidate
                  <span className="text-blue-600">{getSortIcon('candidate')}</span>
                </div>
              </th>
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => onSort('committee')}
              >
                <div className="flex items-center gap-1">
                  Committee
                  <span className="text-blue-600">{getSortIcon('committee')}</span>
                </div>
              </th>
              {showEmployer && (
                <th
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => onSort('employer')}
                >
                  <div className="flex items-center gap-1">
                    Employer
                    <span className="text-blue-600">{getSortIcon('employer')}</span>
                  </div>
                </th>
              )}
              {showOccupation && (
                <th
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                  onClick={() => onSort('occupation')}
                >
                  <div className="flex items-center gap-1">
                    Occupation
                    <span className="text-blue-600">{getSortIcon('occupation')}</span>
                  </div>
                </th>
              )}
              <th
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                onClick={() => onSort('location')}
              >
                <div className="flex items-center gap-1">
                  Location
                  <span className="text-blue-600">{getSortIcon('location')}</span>
                </div>
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {paginatedContributions.map((contrib, idx) => (
              <tr key={idx} className="hover:bg-gray-50">
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                  {contrib.contribution_date || 'N/A'}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                  ${(contrib.contribution_amount || 0).toLocaleString()}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                  {contrib.contributor_name || 'N/A'}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {contrib.candidate_id ? (
                    <span
                      className="text-blue-600 hover:text-blue-800 cursor-pointer"
                      onClick={() => navigate(`/candidate/${contrib.candidate_id}`)}
                    >
                      {candidateNames[contrib.candidate_id] || contrib.candidate_id}
                    </span>
                  ) : (
                    'N/A'
                  )}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {contrib.committee_id ? (
                    <span
                      className="text-blue-600 hover:text-blue-800 cursor-pointer"
                      onClick={() => navigate(`/committee/${contrib.committee_id}`)}
                    >
                      {committeeNames[contrib.committee_id] || contrib.committee_id}
                    </span>
                  ) : (
                    'N/A'
                  )}
                </td>
                {showEmployer && (
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {contrib.contributor_employer || 'N/A'}
                  </td>
                )}
                {showOccupation && (
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {contrib.contributor_occupation || 'N/A'}
                  </td>
                )}
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {contrib.contributor_city && contrib.contributor_state
                    ? `${contrib.contributor_city}, ${contrib.contributor_state}`
                    : 'N/A'}
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
        totalItems={contributions.length}
        onPageChange={onPageChange}
      />
    </div>
  );
}

