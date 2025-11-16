import { useEffect, useRef, useState } from 'react';
import { Network } from 'vis-network';
import { analysisApi, MoneyFlowGraph } from '../services/api';

interface NetworkGraphProps {
  candidateId: string;
  maxDepth?: number;
  minAmount?: number;
}

export default function NetworkGraph({ candidateId, maxDepth = 2, minAmount = 100 }: NetworkGraphProps) {
  const networkRef = useRef<HTMLDivElement>(null);
  const [graph, setGraph] = useState<MoneyFlowGraph | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [aggregateByEmployer, setAggregateByEmployer] = useState<boolean>(true);

  useEffect(() => {
    if (!candidateId) return;
    
    const abortController = new AbortController();
    
    const fetchGraph = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await analysisApi.getMoneyFlow(candidateId, maxDepth, minAmount, aggregateByEmployer, abortController.signal);
        if (!abortController.signal.aborted) {
          setGraph(data);
        }
      } catch (err: any) {
        // Don't set error if request was aborted
        if (err.name === 'AbortError' || abortController.signal.aborted) {
          return;
        }
        const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to load money flow graph';
        if (!abortController.signal.aborted) {
          setError(errorMessage);
          console.error('Error loading money flow graph:', err);
        }
      } finally {
        if (!abortController.signal.aborted) {
          setLoading(false);
        }
      }
    };

    fetchGraph();
    
    return () => {
      abortController.abort();
    };
  }, [candidateId, maxDepth, minAmount, aggregateByEmployer]);

  useEffect(() => {
    if (!graph || !networkRef.current) return;

    // Prepare nodes for vis-network
    const nodes = graph.nodes.map((node) => {
      const color = node.type === 'candidate' 
        ? '#3B82F6' 
        : node.type === 'committee' 
        ? '#10B981' 
        : node.type === 'employer'
        ? '#8B5CF6'
        : '#F59E0B';
      
      return {
        id: node.id,
        label: node.name.length > 30 ? node.name.substring(0, 30) + '...' : node.name,
        color: {
          background: color,
          border: color,
          highlight: {
            background: color,
            border: color,
          },
        },
        shape: node.type === 'candidate' ? 'diamond' : node.type === 'committee' ? 'box' : 'dot',
        size: node.type === 'candidate' ? 30 : node.type === 'committee' ? 20 : node.type === 'employer' ? 18 : 15,
        title: `${node.name}${node.amount ? ` - $${node.amount.toLocaleString()}` : ''}`,
      };
    });

    // Prepare edges for vis-network
    const edges = graph.edges.map((edge) => ({
      from: edge.source,
      to: edge.target,
      value: Math.log10(edge.amount + 1) * 2, // Scale edge width
      label: `$${(edge.amount / 1000).toFixed(1)}K`,
      title: `$${edge.amount.toLocaleString()}`,
      color: {
        color: '#9CA3AF',
        highlight: '#3B82F6',
      },
    }));

    const data = { nodes, edges };
    const options = {
      nodes: {
        font: {
          size: 12,
          color: '#374151',
        },
        borderWidth: 2,
      },
      edges: {
        width: 2,
        font: {
          size: 10,
          align: 'middle',
        },
        arrows: {
          to: {
            enabled: true,
            scaleFactor: 0.5,
          },
        },
        smooth: {
          type: 'continuous',
        },
      },
      physics: {
        enabled: true,
        stabilization: {
          enabled: true,
          iterations: 100,
        },
      },
      interaction: {
        hover: true,
        tooltipDelay: 200,
        zoomView: true,
        dragView: true,
      },
      layout: {
        improvedLayout: true,
      },
    };

    const network = new Network(networkRef.current, data, options);

    return () => {
      network.destroy();
    };
  }, [graph]);

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="animate-pulse">
          <div className="h-4 bg-gray-200 rounded w-3/4 mb-4"></div>
          <div className="h-96 bg-gray-200 rounded"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Money Flow Network</h2>
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <p className="text-red-800">{error}</p>
        </div>
      </div>
    );
  }

  if (!graph || graph.nodes.length === 0) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Money Flow Network</h2>
        <p className="text-gray-600">No data available for money flow visualization</p>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <div className="flex justify-between items-center mb-4">
        <h2 className="text-xl font-semibold">Money Flow Network</h2>
        <div className="flex items-center gap-4">
          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="checkbox"
              checked={aggregateByEmployer}
              onChange={(e) => setAggregateByEmployer(e.target.checked)}
              className="rounded"
            />
            <span className="text-sm text-gray-600">Group by Employer</span>
          </label>
          <div className="flex gap-4 text-sm">
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 bg-blue-500 rounded"></div>
              <span>Candidate</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 bg-green-500 rounded"></div>
              <span>Committee</span>
            </div>
            {aggregateByEmployer ? (
              <div className="flex items-center gap-2">
                <div className="w-4 h-4 bg-purple-500 rounded-full"></div>
                <span>Employer</span>
              </div>
            ) : (
              <div className="flex items-center gap-2">
                <div className="w-4 h-4 bg-yellow-500 rounded-full"></div>
                <span>Donor</span>
              </div>
            )}
          </div>
        </div>
      </div>
      <div ref={networkRef} className="w-full h-96 border border-gray-200 rounded"></div>
      <div className="mt-4 text-sm text-gray-600">
        <p>Nodes: {graph.nodes.length} | Edges: {graph.edges.length}</p>
        <p className="mt-1">Hover over nodes and edges for details. Drag to rearrange.</p>
      </div>
    </div>
  );
}

