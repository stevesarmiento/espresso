'use client';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { useNetworkStats } from '@/hooks/use-network-stats';
import { formatNumber, formatLargeNumber } from '@/lib/utils/format';
import { Activity, TrendingUp, Zap, Database } from 'lucide-react';
import { Skeleton } from '@/components/ui/skeleton';

export function StatsGrid() {
  const { data: stats, isLoading, error, isFetching } = useNetworkStats();

  if (isLoading) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <Card key={i}>
            <CardHeader className="pb-2">
              <Skeleton className="h-4 w-24" />
            </CardHeader>
            <CardContent>
              <Skeleton className="h-8 w-32" />
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  if (error || !stats) {
    return (
      <div className="text-center py-12">
        <p className="text-destructive">Error loading network stats</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-2 px-3 py-1.5 bg-green-50 border border-green-200 rounded-full">
          <span className={`inline-block w-2 h-2 bg-green-500 rounded-full ${isFetching ? 'animate-pulse' : ''}`}></span>
          <span className="text-sm font-medium text-green-700">LIVE</span>
        </div>
        <span className="text-body-md text-gray-600">
          Updates every 5 seconds
        </span>
      </div>
      
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <StatCard
          icon={<Activity className="h-5 w-5" />}
          label="Avg TPS"
          value={formatNumber(stats.avg_tps)}
          description="Transactions per second"
        />
        
        <StatCard
          icon={<TrendingUp className="h-5 w-5" />}
          label="Peak TPS"
          value={formatNumber(stats.max_tps)}
          description="Maximum recorded"
        />
        
        <StatCard
          icon={<Zap className="h-5 w-5" />}
          label="Total Transactions"
          value={formatLargeNumber(stats.total_transactions)}
          description="Last hour"
        />
        
        <StatCard
          icon={<Database className="h-5 w-5" />}
          label="Slots Processed"
          value={formatLargeNumber(stats.total_slots)}
          description="Last hour"
        />
      </div>
    </div>
  );
}

function StatCard({
  icon,
  label,
  value,
  description,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  description: string;
}) {
  return (
    <div className="group relative p-6 border border-border-low bg-white hover:shadow-lg transition-all duration-200">
      <div className="product-card-diagonal absolute inset-0 pointer-events-none" />
      <div className="relative z-10">
        <div className="flex items-center justify-between mb-4">
          <span className="text-body-md text-gray-600">{label}</span>
          <div className="text-gray-400 group-hover:text-gray-900 transition-colors">
            {icon}
          </div>
        </div>
        <div className="text-3xl font-diatype-medium text-gray-900 mb-2">
          {value}
        </div>
        <p className="text-body-md text-gray-500">{description}</p>
      </div>
    </div>
  );
}

