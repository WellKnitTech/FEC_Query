import { useEffect, useState } from 'react';
import { fraudApi, FraudAnalysis } from '../services/api';
import { Radar } from 'react-chartjs-2';
import { formatDate } from '../utils/dateUtils';
import {
  Chart as ChartJS,
  RadialLinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend,
} from 'chart.js';

ChartJS.register(
  RadialLinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend
);

interface FraudRadarChartProps {
  candidateId: string;
  minDate?: string;
  maxDate?: string;
}

export default function FraudRadarChart({ candidateId, minDate, maxDate }: FraudRadarChartProps) {
  const [analysis, setAnalysis] = useState<FraudAnalysis | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchAnalysis = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await fraudApi.analyze(candidateId, minDate, maxDate);
        setAnalysis(data);
      } catch (err: any) {
        const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to load fraud analysis';
        setError(errorMessage);
        console.error('Error loading fraud analysis:', err);
      } finally {
        setLoading(false);
      }
    };

    if (candidateId) {
      fetchAnalysis();
    }
  }, [candidateId, minDate, maxDate]);

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="animate-pulse">
          <div className="h-4 bg-gray-200 rounded w-3/4 mb-4"></div>
          <div className="h-64 bg-gray-200 rounded"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Fraud Pattern Analysis</h2>
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <p className="text-red-800">{error}</p>
        </div>
      </div>
    );
  }

  if (!analysis || analysis.patterns.length === 0) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Fraud Pattern Analysis</h2>
        <p className="text-gray-600">No fraud patterns detected to visualize</p>
      </div>
    );
  }

  // Group patterns by type and calculate metrics
  const patternTypes = ['smurfing', 'threshold_clustering', 'temporal_anomaly', 'round_number_pattern', 'same_day_multiple'];
  const patternData: Record<string, { count: number; totalAmount: number; avgConfidence: number }> = {};

  for (const pattern of analysis.patterns) {
    const type = pattern.pattern_type;
    if (!patternData[type]) {
      patternData[type] = { count: 0, totalAmount: 0, avgConfidence: 0 };
    }
    patternData[type].count += 1;
    patternData[type].totalAmount += pattern.total_amount;
    patternData[type].avgConfidence += pattern.confidence_score;
  }

  // Calculate averages
  for (const type in patternData) {
    if (patternData[type].count > 0) {
      patternData[type].avgConfidence /= patternData[type].count;
    }
  }

  // Normalize data for radar chart (0-100 scale)
  const maxCount = Math.max(...Object.values(patternData).map((p) => p.count), 1);
  const maxAmount = Math.max(...Object.values(patternData).map((p) => p.totalAmount), 1);

  const labels = patternTypes.map((type) =>
    type.replace(/_/g, ' ').replace(/\b\w/g, (l) => l.toUpperCase())
  );
  const countData = patternTypes.map((type) =>
    patternData[type] ? (patternData[type].count / maxCount) * 100 : 0
  );
  const amountData = patternTypes.map((type) =>
    patternData[type] ? (patternData[type].totalAmount / maxAmount) * 100 : 0
  );
  const confidenceData = patternTypes.map((type) =>
    patternData[type] ? patternData[type].avgConfidence * 100 : 0
  );

  const chartData = {
    labels: labels,
    datasets: [
      {
        label: 'Pattern Count (normalized)',
        data: countData,
        borderColor: 'rgba(239, 68, 68, 1)',
        backgroundColor: 'rgba(239, 68, 68, 0.2)',
        pointBackgroundColor: 'rgba(239, 68, 68, 1)',
        pointBorderColor: '#fff',
        pointHoverBackgroundColor: '#fff',
        pointHoverBorderColor: 'rgba(239, 68, 68, 1)',
      },
      {
        label: 'Total Amount (normalized)',
        data: amountData,
        borderColor: 'rgba(245, 158, 11, 1)',
        backgroundColor: 'rgba(245, 158, 11, 0.2)',
        pointBackgroundColor: 'rgba(245, 158, 11, 1)',
        pointBorderColor: '#fff',
        pointHoverBackgroundColor: '#fff',
        pointHoverBorderColor: 'rgba(245, 158, 11, 1)',
      },
      {
        label: 'Confidence Score',
        data: confidenceData,
        borderColor: 'rgba(59, 130, 246, 1)',
        backgroundColor: 'rgba(59, 130, 246, 0.2)',
        pointBackgroundColor: 'rgba(59, 130, 246, 1)',
        pointBorderColor: '#fff',
        pointHoverBackgroundColor: '#fff',
        pointHoverBorderColor: 'rgba(59, 130, 246, 1)',
      },
    ],
  };

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <div className="mb-4">
        <h2 className="text-xl font-semibold">Fraud Pattern Analysis</h2>
        {minDate && maxDate && (
          <p className="text-sm text-gray-500 mt-1">
            Date Range: {formatDate(minDate)} - {formatDate(maxDate)}
          </p>
        )}
      </div>
      <div className="mb-4">
        <div className="grid grid-cols-3 gap-4 mb-4">
          <div>
            <div className="text-sm text-gray-600">Risk Score</div>
            <div className="text-2xl font-bold">{analysis.risk_score.toFixed(1)}%</div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Patterns Detected</div>
            <div className="text-2xl font-bold">{analysis.patterns.length}</div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Suspicious Amount</div>
            <div className="text-2xl font-bold">${(analysis.total_suspicious_amount / 1000).toFixed(1)}K</div>
          </div>
        </div>
      </div>
      <Radar
        data={chartData}
        options={{
          responsive: true,
          maintainAspectRatio: true,
          scales: {
            r: {
              beginAtZero: true,
              max: 100,
              ticks: {
                stepSize: 20,
              },
            },
          },
        }}
      />
      <div className="mt-4 text-sm text-gray-600">
        <p className="mb-2">Pattern Summary:</p>
        <div className="grid grid-cols-2 md:grid-cols-5 gap-2">
          {Object.entries(patternData).map(([type, data]) => (
            <div key={type} className="p-2 bg-gray-50 rounded">
              <div className="font-medium text-xs">
                {type.replace(/_/g, ' ').replace(/\b\w/g, (l) => l.toUpperCase())}
              </div>
              <div className="text-xs">
                Count: {data.count}
                <br />
                Amount: ${(data.totalAmount / 1000).toFixed(1)}K
                <br />
                Confidence: {(data.avgConfidence * 100).toFixed(0)}%
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

