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
      <Card>
        <CardHeader>
          <CardTitle>TPS Over Time</CardTitle>
        </CardHeader>
        <CardContent>
          <Skeleton className="h-[300px] w-full" />
        </CardContent>
      </Card>
    );
  }

  if (error || !tpsData || tpsData.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>TPS Over Time</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground text-sm">No data available</p>
        </CardContent>
      </Card>
    );
  }

  const chartData = tpsData.map((item) => ({
    timestamp: formatDate(item.timestamp, 'HH:mm'),
    tps: item.tps,
  }));

  return (
    <Card>
      <CardHeader>
        <CardTitle>TPS Over Time</CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="timestamp"
              tick={{ fontSize: 12 }}
              interval="preserveStartEnd"
            />
            <YAxis tick={{ fontSize: 12 }} />
            <Tooltip />
            <Line
              type="monotone"
              dataKey="tps"
              stroke="hsl(var(--primary))"
              strokeWidth={2}
              name="TPS"
              dot={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
}

