import { DataTypeStatus } from '../services/api';

interface DataTypeStatusBadgeProps {
  status: DataTypeStatus;
}

export default function DataTypeStatusBadge({ status }: DataTypeStatusBadgeProps) {
  const getStatusColor = () => {
    if (!status.is_implemented) {
      return 'bg-yellow-100 text-yellow-800';
    }
    switch (status.status) {
      case 'imported':
        return 'bg-green-100 text-green-800';
      case 'in_progress':
        return 'bg-blue-100 text-blue-800';
      case 'failed':
        return 'bg-red-100 text-red-800';
      case 'not_imported':
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  const getStatusText = () => {
    if (!status.is_implemented) {
      return 'Not Implemented';
    }
    switch (status.status) {
      case 'imported':
        return 'Imported';
      case 'in_progress':
        return 'In Progress';
      case 'failed':
        return 'Failed';
      case 'not_imported':
      default:
        return 'Not Imported';
    }
  };

  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getStatusColor()}`}>
      {getStatusText()}
    </span>
  );
}

