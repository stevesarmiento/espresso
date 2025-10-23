'use client';

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { useTPSHistory } from '@/hooks/use-network-stats';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import { formatDate } from '@/lib/utils/format';
import { Skeleton } from '@/components/ui/skeleton';

interface TPSChartProps {
  hours?: number;
}

export function TPSChart({ hours = 24 }: TPSChartProps) {
  const { data: tpsData, isLoading, error } = useTPSHistory(hours);

  if (isLoading) {
    return (
      <div className="group relative p-6 border border-border-low bg-white">
        <div className="product-card-diagonal absolute inset-0 pointer-events-none" />
        <div className="relative z-10">
          <h3 className="text-title-5 mb-6">TPS Over Time</h3>
          <Skeleton className="h-[300px] w-full" />
        </div>
      </div>
    );
  }

  if (error || !tpsData || tpsData.length === 0) {
    return (
      <div className="group relative p-6 border border-border-low bg-white">
        <div className="product-card-diagonal absolute inset-0 pointer-events-none" />
        <div className="relative z-10">
          <h3 className="text-title-5 mb-6">TPS Over Time</h3>
          <p className="text-body-md text-gray-600">No data available</p>
        </div>
      </div>
    );
  }

  const chartData = tpsData.map((item) => ({
    timestamp: formatDate(item.timestamp, 'HH:mm'),
    tps: item.tps,
  }));

  return (
    <div className="group relative p-6 border border-border-low bg-white hover:shadow-lg transition-all duration-200">
      <div className="product-card-diagonal absolute inset-0 pointer-events-none" />
      <div className="relative z-10">
        <h3 className="text-title-5 mb-6">TPS Over Time</h3>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(233, 231, 222, 1)" />
            <XAxis
              dataKey="timestamp"
              tick={{ fontSize: 12, fill: '#666' }}
              interval="preserveStartEnd"
            />
            <YAxis tick={{ fontSize: 12, fill: '#666' }} />
            <Tooltip />
            <Line
              type="monotone"
              dataKey="tps"
              stroke="rgb(0, 0, 0)"
              strokeWidth={2}
              name="TPS"
              dot={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

