import { useState, useCallback } from 'react';

interface UsePaginationResult {
  currentPage: number;
  itemsPerPage: number;
  sortColumn: string | null;
  sortDirection: 'asc' | 'desc';
  handleSort: (column: string) => void;
  setPage: (page: number) => void;
  setItemsPerPage: (itemsPerPage: number) => void;
  getSortIcon: (column: string) => string;
  resetPagination: () => void;
}

export function usePagination(): UsePaginationResult {
  const [currentPage, setCurrentPage] = useState<number>(1);
  const [itemsPerPage, setItemsPerPage] = useState<number>(25);
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('desc');

  const handleSort = useCallback((column: string) => {
    if (sortColumn === column) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortColumn(column);
      setSortDirection('desc');
    }
    setCurrentPage(1); // Reset to first page when sorting
  }, [sortColumn, sortDirection]);

  const setPage = useCallback((page: number) => {
    setCurrentPage(page);
  }, []);

  const setItemsPerPageValue = useCallback((itemsPerPage: number) => {
    setItemsPerPage(itemsPerPage);
    setCurrentPage(1); // Reset to first page when changing items per page
  }, []);

  const getSortIcon = useCallback((column: string): string => {
    if (sortColumn !== column) {
      return '↕';
    }
    return sortDirection === 'asc' ? '↑' : '↓';
  }, [sortColumn, sortDirection]);

  const resetPagination = useCallback(() => {
    setCurrentPage(1);
    setSortColumn(null);
    setSortDirection('desc');
  }, []);

  return {
    currentPage,
    itemsPerPage,
    sortColumn,
    sortDirection,
    handleSort,
    setPage,
    setItemsPerPage: setItemsPerPageValue,
    getSortIcon,
    resetPagination,
  };
}

