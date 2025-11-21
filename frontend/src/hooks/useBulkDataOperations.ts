import { useState, useCallback } from 'react';
import { bulkDataApi } from '../services/api';

interface UseBulkDataOperationsProps {
  selectedCycle: number;
  selectedDataTypes: Set<string>;
  forceDownload: boolean;
  onSuccess?: (message: string) => void;
  onError?: (message: string) => void;
  onJobStarted?: (jobId: string) => void;
  onStatusRefresh?: () => void;
  onCycleStatusRefresh?: () => void;
}

interface UseBulkDataOperationsReturn {
  loading: boolean;
  operationType: string | null;
  handleDownload: () => Promise<void>;
  handleImportSelected: () => Promise<void>;
  handleImportAllTypes: () => Promise<void>;
  handleClearContributions: () => Promise<void>;
  handleCleanupAndReimport: () => Promise<void>;
  handleImportAll: () => Promise<void>;
  handleRefreshCycles: () => Promise<void>;
  handleComputeAnalysis: () => Promise<void>;
}

/**
 * Custom hook to consolidate all bulk data operation handlers
 */
export function useBulkDataOperations({
  selectedCycle,
  selectedDataTypes,
  forceDownload,
  onSuccess,
  onError,
  onJobStarted,
  onStatusRefresh,
  onCycleStatusRefresh,
}: UseBulkDataOperationsProps): UseBulkDataOperationsReturn {
  const [loading, setLoading] = useState(false);
  const [operationType, setOperationType] = useState<string | null>(null);

  const extractErrorMessage = (err: any): string => {
    if (!err) return 'An unknown error occurred';

    // Handle FastAPI validation errors (422) - detail is an array of error objects
    if (err.response?.data?.detail) {
      const detail = err.response.data.detail;

      // If detail is an array (validation errors), format them
      if (Array.isArray(detail)) {
        return detail
          .map((error: any) => {
            const loc = error.loc ? error.loc.join('.') : '';
            const msg = error.msg || error.message || 'Validation error';
            return loc ? `${loc}: ${msg}` : msg;
          })
          .join('; ');
      }

      // If detail is a string, use it directly
      if (typeof detail === 'string') {
        return detail;
      }
    }

    // Fallback to message or default
    return err.message || 'An unknown error occurred';
  };

  const handleDownload = useCallback(async () => {
    if (!window.confirm(`Download and import bulk data for cycle ${selectedCycle}?`)) {
      return;
    }

    try {
      setLoading(true);
      setOperationType('download');
      onError?.(null as any);

      const result = await bulkDataApi.download(selectedCycle, forceDownload);
      const message = result.message || `Download started for cycle ${selectedCycle}`;
      onSuccess?.(message);

      if (result.job_id) {
        onJobStarted?.(result.job_id);
      }

      onStatusRefresh?.();
      onCycleStatusRefresh?.();
    } catch (err: any) {
      const errorMessage = extractErrorMessage(err) || 'Failed to download bulk data';
      onError?.(errorMessage);
      console.error('Download error:', err);
    } finally {
      setLoading(false);
      setOperationType(null);
    }
  }, [selectedCycle, forceDownload, onSuccess, onError, onJobStarted, onStatusRefresh, onCycleStatusRefresh]);

  const handleImportSelected = useCallback(async () => {
    if (selectedDataTypes.size === 0) {
      onError?.('Please select at least one data type to import');
      return;
    }

    const dataTypesArray = Array.from(selectedDataTypes);
    if (
      !window.confirm(`Import ${dataTypesArray.length} selected data type(s) for cycle ${selectedCycle}?`)
    ) {
      return;
    }

    try {
      setLoading(true);
      setOperationType('import-selected');
      onError?.(null as any);

      const result = await bulkDataApi.importMultipleDataTypes(
        selectedCycle,
        dataTypesArray,
        forceDownload
      );
      const message = result.message || `Import started for ${dataTypesArray.length} data types`;
      onSuccess?.(message);

      if (result.job_id) {
        onJobStarted?.(result.job_id);
      }

      onCycleStatusRefresh?.();
    } catch (err: any) {
      const errorMessage =
        extractErrorMessage(err) || 'Failed to import selected data types';
      onError?.(errorMessage);
      console.error('Import error:', err);
    } finally {
      setLoading(false);
      setOperationType(null);
    }
  }, [
    selectedCycle,
    selectedDataTypes,
    forceDownload,
    onSuccess,
    onError,
    onJobStarted,
    onCycleStatusRefresh,
  ]);

  const handleImportAllTypes = useCallback(async () => {
    if (!window.confirm(`Import all implemented data types for cycle ${selectedCycle}?`)) {
      return;
    }

    try {
      setLoading(true);
      setOperationType('import-all-types');
      onError?.(null as any);

      const result = await bulkDataApi.importAllDataTypes(selectedCycle);
      const message = result.message || `Import started for all data types`;
      onSuccess?.(message);

      if (result.job_id) {
        onJobStarted?.(result.job_id);
      }

      onCycleStatusRefresh?.();
    } catch (err: any) {
      const errorMessage = extractErrorMessage(err) || 'Failed to import all data types';
      onError?.(errorMessage);
      console.error(err);
    } finally {
      setLoading(false);
      setOperationType(null);
    }
  }, [selectedCycle, onSuccess, onError, onJobStarted, onCycleStatusRefresh]);

  const handleClearContributions = useCallback(async () => {
    if (
      !window.confirm(
        'Are you sure you want to clear all contributions? This action cannot be undone.'
      )
    ) {
      return;
    }

    try {
      setLoading(true);
      setOperationType('clear');
      onError?.(null as any);

      const result = await bulkDataApi.clearContributions();
      const message = `Successfully cleared ${result.deleted_count} contributions`;
      onSuccess?.(message);

      onStatusRefresh?.();
    } catch (err: any) {
      const errorMessage = extractErrorMessage(err) || 'Failed to clear contributions';
      onError?.(errorMessage);
      console.error(err);
    } finally {
      setLoading(false);
      setOperationType(null);
    }
  }, [onSuccess, onError, onStatusRefresh]);

  const handleCleanupAndReimport = useCallback(async () => {
    if (
      !window.confirm(
        `Are you sure you want to clear all contributions and reimport cycle ${selectedCycle}? ` +
          'This will delete all existing contribution data and cannot be undone.'
      )
    ) {
      return;
    }

    try {
      setLoading(true);
      setOperationType('cleanup-reimport');
      onError?.(null as any);

      const result = await bulkDataApi.cleanupAndReimport(selectedCycle);
      const message = result.message || `Cleanup and reimport started for cycle ${selectedCycle}`;
      onSuccess?.(message);

      if (result.job_id) {
        onJobStarted?.(result.job_id);
      }

      onStatusRefresh?.();
    } catch (err: any) {
      const errorMessage = extractErrorMessage(err) || 'Failed to cleanup and reimport';
      onError?.(errorMessage);
      console.error(err);
    } finally {
      setLoading(false);
      setOperationType(null);
    }
  }, [selectedCycle, onSuccess, onError, onJobStarted, onStatusRefresh]);

  const handleImportAll = useCallback(async () => {
    const endYear = new Date().getFullYear() + 6;
    if (
      !window.confirm(
        `Are you sure you want to import all cycles from 2000 to ${endYear}? ` +
          'This will download and import data for all election cycles (including future cycles). This may take a very long time.'
      )
    ) {
      return;
    }

    try {
      setLoading(true);
      setOperationType('import-all');
      onError?.(null as any);

      const result = await bulkDataApi.importAll();
      const message = result.message || `Import started for ${result.count} cycles`;
      onSuccess?.(message);

      if (result.job_id) {
        onJobStarted?.(result.job_id);
      }

      onStatusRefresh?.();
    } catch (err: any) {
      const errorMessage = extractErrorMessage(err) || 'Failed to import all cycles';
      onError?.(errorMessage);
      console.error(err);
    } finally {
      setLoading(false);
      setOperationType(null);
    }
  }, [onSuccess, onError, onJobStarted, onStatusRefresh]);

  const handleRefreshCycles = useCallback(async () => {
    try {
      setLoading(true);
      setOperationType('refresh-cycles');
      onError?.(null as any);

      const result = await bulkDataApi.refreshCycles();
      const message = `Found ${result.count} available cycles. Cycles updated in database.`;
      onSuccess?.(message);

      onStatusRefresh?.();
    } catch (err: any) {
      const errorMessage = extractErrorMessage(err) || 'Failed to refresh cycles';
      onError?.(errorMessage);
      console.error(err);
    } finally {
      setLoading(false);
      setOperationType(null);
    }
  }, [onSuccess, onError, onStatusRefresh]);

  const handleComputeAnalysis = useCallback(async () => {
    if (!window.confirm(`Compute analysis for cycle ${selectedCycle}? This will pre-compute employer, velocity, and donor state analyses for all candidates in this cycle.`)) {
      return;
    }

    try {
      setLoading(true);
      setOperationType('compute-analysis');
      onError?.(null as any);

      const result = await bulkDataApi.computeAnalysis(selectedCycle);
      const message = result.message || `Analysis computation started for cycle ${selectedCycle}`;
      onSuccess?.(message);

      onCycleStatusRefresh?.();
    } catch (err: any) {
      const errorMessage = extractErrorMessage(err) || 'Failed to compute analysis';
      onError?.(errorMessage);
      console.error('Analysis computation error:', err);
    } finally {
      setLoading(false);
      setOperationType(null);
    }
  }, [selectedCycle, onSuccess, onError, onCycleStatusRefresh]);

  return {
    loading,
    operationType,
    handleDownload,
    handleImportSelected,
    handleImportAllTypes,
    handleClearContributions,
    handleCleanupAndReimport,
    handleImportAll,
    handleRefreshCycles,
    handleComputeAnalysis,
  };
}

