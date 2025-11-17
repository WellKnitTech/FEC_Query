import { ReactNode } from 'react';
import LoadingState from './LoadingState';
import ErrorState from './ErrorState';

interface AnalysisSectionProps {
  title: string;
  loading?: boolean;
  error?: string | null;
  onRetry?: () => void;
  children: ReactNode;
  className?: string;
}

export default function AnalysisSection({
  title,
  loading = false,
  error = null,
  onRetry,
  children,
  className = '',
}: AnalysisSectionProps) {
  if (loading) {
    return <LoadingState message={`Loading ${title}...`} className={className} />;
  }

  if (error) {
    return (
      <ErrorState 
        title={`Error Loading ${title}`} 
        error={error} 
        onRetry={onRetry}
        className={className}
      />
    );
  }

  return (
    <div className={`bg-white rounded-lg shadow p-6 ${className}`}>
      <h2 className="text-xl font-semibold mb-4">{title}</h2>
      {children}
    </div>
  );
}

