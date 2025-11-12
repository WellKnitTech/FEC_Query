import { useState, useEffect } from 'react';
import { analysisApi } from '../services/api';
import Plot from 'react-plotly.js';

interface GeographicHeatmapProps {
  candidateId?: string;
  committeeId?: string;
  minDate?: string;
  maxDate?: string;
}

export default function GeographicHeatmap({
  candidateId,
  committeeId,
  minDate,
  maxDate,
}: GeographicHeatmapProps) {
  const [geographicData, setGeographicData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedState, setSelectedState] = useState<string | null>(null);

  useEffect(() => {
    const fetchGeographicData = async () => {
      setLoading(true);
      setError(null);
      try {
        const params = {
          candidate_id: candidateId,
          committee_id: committeeId,
          min_date: minDate,
          max_date: maxDate,
        };
        console.log('[GeographicHeatmap] Fetching geographic data with params:', params);
        
        const data = await analysisApi.getGeographic(params);
        console.log('[GeographicHeatmap] Received geographic data:', {
          total_contributions: data.total_contributions,
          states_count: Object.keys(data.contributions_by_state).length,
          cities_count: Object.keys(data.contributions_by_city).length,
        });
        setGeographicData(data);
      } catch (err: any) {
        const errorMessage = err?.response?.data?.detail || err?.message || 'Unknown error';
        const statusCode = err?.response?.status;
        console.error('[GeographicHeatmap] Error loading geographic data:', {
          status: statusCode,
          message: errorMessage,
          error: err,
        });
        setError(`Failed to load geographic data${statusCode ? ` (${statusCode})` : ''}: ${errorMessage}`);
      } finally {
        setLoading(false);
      }
    };

    if (candidateId || committeeId) {
      fetchGeographicData();
    }
  }, [candidateId, committeeId, minDate, maxDate]);

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="animate-pulse">
          <div className="h-4 bg-gray-200 rounded w-1/4 mb-4"></div>
          <div className="h-96 bg-gray-200 rounded"></div>
        </div>
      </div>
    );
  }

  if (error || !geographicData) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="text-red-600">{error || 'No geographic data available'}</div>
      </div>
    );
  }

  // Prepare data for Plotly choropleth map
  const states = Object.keys(geographicData.contributions_by_state);
  const amounts = states.map(state => geographicData.contributions_by_state[state] || 0);

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Geographic Contribution Heatmap</h2>
      
      <div className="mb-4">
        <div className="text-sm text-gray-600 mb-2">
          Total Contributions: ${(geographicData.total_contributions / 1000).toFixed(1)}K
        </div>
        {selectedState && (
          <div className="text-sm font-medium text-blue-600">
            {selectedState}: ${((geographicData.contributions_by_state[selectedState] || 0) / 1000).toFixed(1)}K
          </div>
        )}
      </div>

      <div className="mb-6">
        <Plot
          data={[
            {
              type: 'choropleth',
              locationmode: 'USA-states',
              locations: states,
              z: amounts,
              text: states.map(state => 
                `${state}: $${((geographicData.contributions_by_state[state] || 0) / 1000).toFixed(1)}K`
              ),
              colorscale: [
                [0, 'rgb(229, 231, 235)'],
                [0.2, 'rgb(191, 219, 254)'],
                [0.4, 'rgb(147, 197, 253)'],
                [0.6, 'rgb(96, 165, 250)'],
                [0.8, 'rgb(59, 130, 246)'],
                [1, 'rgb(37, 99, 235)'],
              ],
              colorbar: {
                title: 'Contributions ($)',
                tickformat: ',.0f',
              },
              marker: {
                line: {
                  color: 'white',
                  width: 1,
                },
              },
            },
          ]}
          layout={{
            title: 'Contributions by State',
            geo: {
              scope: 'usa',
              projection: { type: 'albers usa' },
              showlakes: true,
              lakecolor: 'rgb(255, 255, 255)',
            },
            height: 400,
            margin: { t: 50, b: 0, l: 0, r: 0 },
          }}
          config={{ responsive: true }}
          style={{ width: '100%', height: '400px' }}
        />
      </div>

      <div className="mt-4">
        <h3 className="text-sm font-semibold mb-2">Top States</h3>
        <div className="space-y-1">
          {geographicData.top_states.slice(0, 10).map((state: any, idx: number) => (
            <div key={idx} className="flex justify-between text-sm">
              <span>{state.state}</span>
              <span className="font-medium">${(state.total / 1000).toFixed(1)}K</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

