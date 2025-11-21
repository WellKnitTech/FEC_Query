import { Contribution, AggregatedDonor } from '../../services/api';
import { calculateSummaryStats } from '../../utils/donorAnalysisUtils';

interface SummaryCardsProps {
  contributions: Contribution[];
  aggregatedDonors: AggregatedDonor[];
  viewAggregated: boolean;
}

export default function SummaryCards({
  contributions,
  aggregatedDonors,
  viewAggregated,
}: SummaryCardsProps) {
  const stats = calculateSummaryStats(contributions, aggregatedDonors, viewAggregated);

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Summary</h2>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div>
          <div className="text-sm text-gray-600">Total Contributions</div>
          <div className="text-2xl font-bold">${stats.totalAmount.toLocaleString()}</div>
        </div>
        <div>
          <div className="text-sm text-gray-600">
            {viewAggregated ? 'Unique Donors' : 'Number of Contributions'}
          </div>
          <div className="text-2xl font-bold">
            {viewAggregated ? stats.uniqueDonors : stats.totalContributions}
          </div>
        </div>
        {viewAggregated && (
          <div>
            <div className="text-sm text-gray-600">Name Variations Found</div>
            <div className="text-2xl font-bold">{stats.nameVariations}</div>
          </div>
        )}
        {!viewAggregated && (
          <div>
            <div className="text-sm text-gray-600">Candidates Supported</div>
            <div className="text-2xl font-bold">{stats.uniqueCandidates}</div>
          </div>
        )}
        <div>
          <div className="text-sm text-gray-600">Average Contribution</div>
          <div className="text-2xl font-bold">${stats.averageContribution.toFixed(2)}</div>
        </div>
      </div>
    </div>
  );
}

