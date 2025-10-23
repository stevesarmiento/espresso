'use client';

import { useNetworkStats } from '@/hooks/use-network-stats';
import { formatNumber, formatLargeNumber } from '@/lib/utils/format';
import { Activity, TrendingUp } from 'lucide-react';

export function CurrentStats() {
  const { data: stats, isLoading, error, isFetching } = useNetworkStats();

  if (isLoading) {
    return (
      <div className="group relative p-8 border border-border-low bg-white">
        <div className="product-card-diagonal absolute inset-0 pointer-events-none" />
        <div className="relative z-10">
          <p className="text-body-md text-gray-600">Loading current stats...</p>
        </div>
      </div>
    );
  }

  if (error || !stats) {
    return (
      <div className="group relative p-8 border border-border-low bg-white">
        <div className="product-card-diagonal absolute inset-0 pointer-events-none" />
        <div className="relative z-10">
          <p className="text-body-md text-red-600">Error loading stats</p>
        </div>
      </div>
    );
  }

  return (
    <div className="group relative border border-border-low bg-white hover:shadow-lg transition-all duration-200">
      <div className="product-card-diagonal absolute inset-0 pointer-events-none" />
      
      <div className="relative z-10 p-8">
        {/* Live Indicator */}
        <div className="flex items-center gap-3 mb-8">
          <div className="flex items-center gap-2 px-3 py-1.5 bg-green-50 border border-green-200 rounded-full">
            <span className={`inline-block w-2 h-2 bg-green-500 rounded-full ${isFetching ? 'animate-pulse' : ''}`}></span>
            <span className="text-sm font-medium text-green-700">LIVE</span>
          </div>
          <span className="text-body-md text-gray-600">
            Updates every second
          </span>
        </div>

        {/* Main Stats Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
          {/* Current TPS - Big and prominent */}
          <div>
            <div className="flex items-center gap-3 mb-3">
              <Activity className="h-6 w-6 text-gray-400" />
              <span className="text-body-l text-gray-600">Current TPS</span>
            </div>
            <div className="text-[64px] font-diatype-medium leading-none text-gray-900 mb-2">
              {formatNumber(stats.avg_tps, 0)}
            </div>
            <p className="text-body-md text-gray-500">
              Transactions per second
            </p>
          </div>

          {/* Peak TPS */}
          <div>
            <div className="flex items-center gap-3 mb-3">
              <TrendingUp className="h-6 w-6 text-gray-400" />
              <span className="text-body-l text-gray-600">Peak TPS</span>
            </div>
            <div className="text-[64px] font-diatype-medium leading-none text-gray-900 mb-2">
              {formatNumber(stats.max_tps, 0)}
            </div>
            <p className="text-body-md text-gray-500">
              Maximum in last hour
            </p>
          </div>
        </div>

        {/* Secondary Stats */}
        <div className="grid grid-cols-2 gap-6 mt-8 pt-8 border-t border-border-extra-low">
          <div>
            <p className="text-body-md text-gray-600 mb-2">Total Transactions</p>
            <p className="text-2xl font-diatype-medium text-gray-900">
              {formatLargeNumber(stats.total_transactions)}
            </p>
          </div>
          <div>
            <p className="text-body-md text-gray-600 mb-2">Slots Processed</p>
            <p className="text-2xl font-diatype-medium text-gray-900">
              {formatLargeNumber(stats.total_slots)}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

