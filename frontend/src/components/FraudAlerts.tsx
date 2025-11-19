import { useState } from 'react';
import { useFraudAnalysis } from '../hooks/useFraudAnalysis';
import { formatDate } from '../utils/dateUtils';

interface FraudAlertsProps {
  candidateId: string;
  minDate?: string;
  maxDate?: string;
}

export default function FraudAlerts({ candidateId, minDate, maxDate }: FraudAlertsProps) {
  const [expandedPattern, setExpandedPattern] = useState<string | null>(null);
  const [useAggregation, setUseAggregation] = useState<boolean>(true);
  
  // Use shared hook with aggregation option
  const { analysis, loading, error, refresh } = useFraudAnalysis(candidateId, minDate, maxDate, useAggregation);

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="animate-pulse">
          <div className="h-4 bg-gray-200 rounded w-3/4 mb-4"></div>
          <div className="h-32 bg-gray-200 rounded"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Fraud Detection Analysis</h2>
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <p className="text-red-800">{error}</p>
        </div>
      </div>
    );
  }

  if (!analysis) {
    return null;
  }

  const getSeverityColor = (severity: string) => {
    switch (severity) {
      case 'high':
        return 'bg-red-100 border-red-400 text-red-800';
      case 'medium':
        return 'bg-yellow-100 border-yellow-400 text-yellow-800';
      case 'low':
        return 'bg-blue-100 border-blue-400 text-blue-800';
      default:
        return 'bg-gray-100 border-gray-400 text-gray-800';
    }
  };

  const getRiskColor = (score: number) => {
    if (score >= 70) return 'bg-red-500';
    if (score >= 40) return 'bg-yellow-500';
    return 'bg-green-500';
  };

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <div className="flex justify-between items-center mb-6">
        <div>
          <h2 className="text-xl font-semibold">Fraud Detection Analysis</h2>
          {minDate && maxDate && (
            <p className="text-sm text-gray-500 mt-1">
              Date Range: {formatDate(minDate)} - {formatDate(maxDate)}
            </p>
          )}
        </div>
        <div className="flex items-center gap-4">
          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="checkbox"
              checked={useAggregation}
              onChange={(e) => setUseAggregation(e.target.checked)}
              className="rounded"
            />
            <span className="text-sm text-gray-600">Use Aggregated Donors</span>
          </label>
          <div>
            <div className="text-sm text-gray-600">Risk Score</div>
            <div className="flex items-center gap-2">
              <div className="w-32 bg-gray-200 rounded-full h-4">
                <div
                  className={`h-4 rounded-full ${getRiskColor(analysis.risk_score)}`}
                  style={{ width: `${Math.min(analysis.risk_score, 100)}%` }}
                ></div>
              </div>
              <span className="text-lg font-bold">{analysis.risk_score.toFixed(1)}%</span>
            </div>
          </div>
        </div>
      </div>

      {analysis.aggregation_enabled && (
        <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded-lg">
          <div className="flex items-center gap-2 text-sm text-blue-800">
            <span className="font-semibold">✓ Aggregated Analysis Enabled</span>
            {analysis.aggregated_donors_count !== undefined && (
              <span className="text-blue-600">
                ({analysis.aggregated_donors_count} unique donors identified)
              </span>
            )}
          </div>
        </div>
      )}

      <div className="mb-4 p-4 bg-gray-50 rounded-lg">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div>
            <div className="text-sm text-gray-600">Total Suspicious Amount</div>
            <div className="text-2xl font-bold">
              ${(analysis.total_suspicious_amount / 1000).toFixed(1)}K
            </div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Patterns Detected</div>
            <div className="text-2xl font-bold">{analysis.patterns.length}</div>
          </div>
          {analysis.aggregated_donors_count !== undefined && (
            <div>
              <div className="text-sm text-gray-600">Unique Donors</div>
              <div className="text-2xl font-bold">{analysis.aggregated_donors_count}</div>
            </div>
          )}
        </div>
      </div>

      {analysis.patterns.length === 0 ? (
        <div className="text-center py-8 text-gray-600">
          <p className="text-lg">No suspicious patterns detected</p>
          <p className="text-sm mt-2">This analysis looks clean based on current detection algorithms.</p>
        </div>
      ) : (
        <div className="space-y-4">
          {analysis.patterns.map((pattern, index) => (
            <div
              key={index}
              className={`border-2 rounded-lg p-4 ${getSeverityColor(pattern.severity)}`}
            >
              <div className="flex justify-between items-start">
                <div className="flex-1">
                  <div className="flex items-center gap-2 mb-2">
                    <span className="px-2 py-1 bg-white rounded text-xs font-semibold uppercase">
                      {pattern.severity}
                    </span>
                    <span className="px-2 py-1 bg-white rounded text-xs font-semibold">
                      {pattern.pattern_type.replace(/_/g, ' ').toUpperCase()}
                    </span>
                    {(pattern.pattern_type.includes('aggregate') || 
                      pattern.pattern_type === 'name_variation_fraud' ||
                      pattern.pattern_type === 'coordinated_contributions' ||
                      pattern.pattern_type === 'rapid_sequential') && (
                      <span className="px-2 py-1 bg-blue-100 text-blue-800 rounded text-xs font-semibold">
                        AGGREGATED
                      </span>
                    )}
                    <span className="text-sm">
                      Confidence: {(pattern.confidence_score * 100).toFixed(0)}%
                    </span>
                  </div>
                  <p className="font-medium mb-2">{pattern.description}</p>
                  <p className="text-sm mb-2">
                    Total Amount: ${pattern.total_amount.toLocaleString()} | 
                    Affected Contributions: {pattern.affected_contributions.length}
                  </p>
                  {expandedPattern === `${index}` && (
                    <div className="mt-4 bg-white rounded p-4 max-h-64 overflow-y-auto">
                      <h4 className="font-semibold mb-2">Affected Contributions:</h4>
                      <div className="space-y-2">
                        {pattern.affected_contributions.slice(0, 20).map((contrib, idx) => (
                          <div key={idx} className="text-xs border-b pb-2">
                            <div className="font-medium">{contrib.contributor_name || 'Unknown'}</div>
                            <div className="text-gray-600">
                              ${contrib.contribution_amount?.toLocaleString()} on{' '}
                              {formatDate(contrib.contribution_date)}
                              {contrib.contributor_city && contrib.contributor_state && (
                                <> from {contrib.contributor_city}, {contrib.contributor_state}</>
                              )}
                            </div>
                          </div>
                        ))}
                        {pattern.affected_contributions.length > 20 && (
                          <div className="text-xs text-gray-500 italic">
                            ... and {pattern.affected_contributions.length - 20} more
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              </div>
              <button
                onClick={() =>
                  setExpandedPattern(expandedPattern === `${index}` ? null : `${index}`)
                }
                className="mt-2 text-sm underline hover:no-underline"
              >
                {expandedPattern === `${index}` ? 'Hide Details' : 'Show Details'}
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

