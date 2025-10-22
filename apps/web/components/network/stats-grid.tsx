'use client';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { useNetworkStats } from '@/hooks/use-network-stats';
import { formatNumber, formatLargeNumber } from '@/lib/utils/format';
import { Activity, TrendingUp, Zap, Database } from 'lucide-react';
import { Skeleton } from '@/components/ui/skeleton';

export function StatsGrid() {
  const { data: stats, isLoading, error } = useNetworkStats();

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
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
      <Card>
        <CardHeader className="pb-2 flex flex-row items-center justify-between space-y-0">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Avg TPS
          </CardTitle>
          <Activity className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{formatNumber(stats.avg_tps)}</div>
          <p className="text-xs text-muted-foreground mt-1">
            Transactions per second
          </p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-2 flex flex-row items-center justify-between space-y-0">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Peak TPS
          </CardTitle>
          <TrendingUp className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">{formatNumber(stats.max_tps)}</div>
          <p className="text-xs text-muted-foreground mt-1">Maximum recorded</p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-2 flex flex-row items-center justify-between space-y-0">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Total Transactions
          </CardTitle>
          <Zap className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">
            {formatLargeNumber(stats.total_transactions)}
          </div>
          <p className="text-xs text-muted-foreground mt-1">Last hour</p>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-2 flex flex-row items-center justify-between space-y-0">
          <CardTitle className="text-sm font-medium text-muted-foreground">
            Slots Processed
          </CardTitle>
          <Database className="h-4 w-4 text-muted-foreground" />
        </CardHeader>
        <CardContent>
          <div className="text-2xl font-bold">
            {formatLargeNumber(stats.total_slots)}
          </div>
          <p className="text-xs text-muted-foreground mt-1">Last hour</p>
        </CardContent>
      </Card>
    </div>
  );
}

