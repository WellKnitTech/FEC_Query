import { useNavigate } from 'react-router-dom';
import { TopEntity } from '../../utils/donorAnalysisUtils';

interface TopEntitiesProps {
  topCandidates: TopEntity[];
  topCommittees: TopEntity[];
  candidateNames: Record<string, string>;
  committeeNames: Record<string, string>;
}

export default function TopEntities({
  topCandidates,
  topCommittees,
  candidateNames,
  committeeNames,
}: TopEntitiesProps) {
  const navigate = useNavigate();

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
      {topCandidates.length > 0 && (
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Top Candidates Supported</h2>
          <div className="space-y-2">
            {topCandidates.map((candidate, idx) => (
              <div
                key={candidate.candidateId}
                className="flex justify-between items-center p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer"
                onClick={() => candidate.candidateId && navigate(`/candidate/${candidate.candidateId}`)}
              >
                <div>
                  <div className="font-medium">
                    #{idx + 1} {candidateNames[candidate.candidateId || ''] || candidate.candidateId}
                  </div>
                  <div className="text-sm text-gray-600">{candidate.count} contributions</div>
                  {candidateNames[candidate.candidateId || ''] && (
                    <div className="text-xs text-gray-500 mt-1">ID: {candidate.candidateId}</div>
                  )}
                </div>
                <div className="text-lg font-bold text-blue-600">
                  ${candidate.amount.toLocaleString()}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {topCommittees.length > 0 && (
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Top Committees Supported</h2>
          <div className="space-y-2">
            {topCommittees.map((committee, idx) => (
              <div
                key={committee.committeeId}
                className="flex justify-between items-center p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer"
                onClick={() => committee.committeeId && navigate(`/committee/${committee.committeeId}`)}
              >
                <div>
                  <div className="font-medium">
                    #{idx + 1} {committeeNames[committee.committeeId || ''] || committee.committeeId}
                  </div>
                  <div className="text-sm text-gray-600">{committee.count} contributions</div>
                  {committeeNames[committee.committeeId || ''] && (
                    <div className="text-xs text-gray-500 mt-1">ID: {committee.committeeId}</div>
                  )}
                </div>
                <div className="text-lg font-bold text-blue-600">
                  ${committee.amount.toLocaleString()}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

