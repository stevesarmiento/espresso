'use client';

import { useTPSHistory } from '@/hooks/use-network-stats';
import { formatNumber, formatSlot, formatDate } from '@/lib/utils/format';
import { Zap } from 'lucide-react';

export function LiveFeed() {
  const { data: tpsData, isLoading, isFetching } = useTPSHistory(1); // Last 1 hour

  if (isLoading) {
    return (
      <div className="group relative p-6 border border-border-low bg-white">
        <div className="product-card-diagonal absolute inset-0 pointer-events-none" />
        <div className="relative z-10">
          <h3 className="text-title-5 mb-6">Live Activity Feed</h3>
          <p className="text-body-md text-gray-600">Loading...</p>
        </div>
      </div>
    );
  }

  if (!tpsData || tpsData.length === 0) {
    return (
      <div className="group relative p-6 border border-border-low bg-white">
        <div className="product-card-diagonal absolute inset-0 pointer-events-none" />
        <div className="relative z-10">
          <h3 className="text-title-5 mb-6">Live Activity Feed</h3>
          <p className="text-body-md text-gray-600">No data yet. Waiting for Jetstreamer...</p>
        </div>
      </div>
    );
  }

  // Get most recent slots (latest first)
  const recentSlots = [...tpsData].reverse().slice(0, 20);

  return (
    <div className="group relative border border-border-low bg-white hover:shadow-lg transition-all duration-200">
      <div className="product-card-diagonal absolute inset-0 pointer-events-none" />
      
      <div className="relative z-10">
        <div className="p-6 border-b border-border-low">
          <div className="flex items-center justify-between">
            <h3 className="text-title-5">Live Activity Feed</h3>
            <div className="flex items-center gap-2">
              <div className={`w-2 h-2 bg-green-500 rounded-full ${isFetching ? 'animate-pulse' : ''}`}></div>
              <span className="text-body-md text-gray-600">Streaming</span>
            </div>
          </div>
        </div>

        <div className="overflow-hidden" style={{ maxHeight: '600px' }}>
          <div className="divide-y divide-border-extra-low">
            {recentSlots.map((slot, index) => (
              <div
                key={`${slot.slot}-${slot.timestamp}`}
                className="p-4 hover:bg-sand-50 transition-colors animate-in fade-in slide-in-from-top-2 duration-300"
                style={{ animationDelay: `${index * 50}ms` }}
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <Zap className="h-4 w-4 text-gray-400" />
                    <div>
                      <p className="font-berkeley-mono text-sm text-gray-900">
                        Slot {formatSlot(slot.slot)}
                      </p>
                      <p className="text-xs text-gray-500 mt-0.5">
                        {formatDate(slot.timestamp, 'HH:mm:ss')}
                      </p>
                    </div>
                  </div>
                  <div className="text-right">
                    <p className="font-diatype-medium text-lg text-gray-900">
                      {formatNumber(slot.tps, 0)}
                    </p>
                    <p className="text-xs text-gray-500">TPS</p>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className="p-4 border-t border-border-low bg-sand-50">
          <p className="text-xs text-gray-500 text-center">
            Showing last 20 slots • Updates every second
          </p>
        </div>
      </div>
    </div>
  );
}

