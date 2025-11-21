interface ViewToggleProps {
  viewAggregated: boolean;
  onToggle: () => void;
  loading: boolean;
}

export default function ViewToggle({ viewAggregated, onToggle, loading }: ViewToggleProps) {
  return (
    <div className="bg-white rounded-lg shadow p-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="checkbox"
              checked={viewAggregated}
              onChange={onToggle}
              disabled={loading}
              className="rounded"
            />
            <span className="text-sm font-medium">View Aggregated Donors</span>
          </label>
          <span className="text-xs text-gray-500">
            {viewAggregated
              ? 'Showing unique donors (grouped by name variations)'
              : 'Showing individual contributions'}
          </span>
        </div>
      </div>
    </div>
  );
}

