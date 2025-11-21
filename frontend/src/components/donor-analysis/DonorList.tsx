import { UniqueContributor } from '../../services/api';

interface DonorListProps {
  contributors: UniqueContributor[];
  onSelect: (contributorName: string) => void;
  searchTerm: string;
}

export default function DonorList({ contributors, onSelect, searchTerm }: DonorListProps) {
  if (contributors.length === 0) return null;

  return (
    <div className="bg-white rounded-lg shadow-lg p-6 mb-6 border-2 border-blue-200">
      <div className="mb-4">
        <h2 className="text-2xl font-bold text-gray-900 mb-2">
          Found {contributors.length} donor{contributors.length !== 1 ? 's' : ''} matching "{searchTerm}"
        </h2>
        <p className="text-sm text-gray-600">
          Click on a donor below to view their contributions
        </p>
      </div>
      <div className="space-y-3 max-h-[500px] overflow-y-auto">
        {contributors.map((contributor, index) => (
          <div
            key={contributor.name}
            className="flex justify-between items-center p-4 border-2 border-gray-200 rounded-lg hover:border-blue-400 hover:bg-blue-50 cursor-pointer transition-all duration-200 shadow-sm hover:shadow-md"
            onClick={() => onSelect(contributor.name)}
          >
            <div className="flex items-center gap-4 flex-1">
              <div className="flex-shrink-0 w-8 h-8 bg-blue-100 rounded-full flex items-center justify-center text-blue-700 font-semibold">
                {index + 1}
              </div>
              <div className="flex-1">
                <div className="font-semibold text-lg text-gray-900">{contributor.name}</div>
                <div className="text-sm text-gray-600 mt-1">
                  {contributor.contribution_count.toLocaleString()} contribution
                  {contributor.contribution_count !== 1 ? 's' : ''}
                </div>
              </div>
            </div>
            <div className="text-right">
              <div className="text-2xl font-bold text-blue-600">
                ${contributor.total_amount.toLocaleString(undefined, {
                  minimumFractionDigits: 2,
                  maximumFractionDigits: 2,
                })}
              </div>
              <div className="text-xs text-gray-500 mt-1">Total contributed</div>
            </div>
          </div>
        ))}
      </div>
      {contributors.length >= 100 && (
        <div className="mt-4 p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
          <p className="text-sm text-yellow-800">
            Showing first 100 results. Try a more specific search term to narrow down results.
          </p>
        </div>
      )}
    </div>
  );
}

