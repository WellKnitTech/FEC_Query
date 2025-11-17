interface ErrorStateProps {
  title?: string;
  error: string | null;
  onRetry?: () => void;
  className?: string;
}

export default function ErrorState({ 
  title = 'Error', 
  error, 
  onRetry,
  className = '' 
}: ErrorStateProps) {
  if (!error) return null;

  return (
    <div className={`bg-white rounded-lg shadow p-6 ${className}`}>
      <div className="bg-red-50 border border-red-200 rounded-lg p-4">
        <h3 className="text-lg font-semibold text-red-800 mb-2">{title}</h3>
        <p className="text-red-700 mb-4">{error}</p>
        {onRetry && (
          <button
            onClick={onRetry}
            className="px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-700 text-sm font-medium"
          >
            Retry
          </button>
        )}
      </div>
    </div>
  );
}

