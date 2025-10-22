import { Suspense } from 'react';
import { StatsGrid } from '@/components/network/stats-grid';
import { TPSChart } from '@/components/network/tps-chart';
import { Skeleton } from '@/components/ui/skeleton';

export const metadata = {
  title: 'Network Health | Solana Analytics',
  description: 'Monitor Solana network health and performance metrics',
};

export default function NetworkPage() {
  return (
    <div className="container mx-auto px-4 py-8">
      <div className="mb-8">
        <h1 className="text-4xl font-bold mb-2">Network Health</h1>
        <p className="text-muted-foreground">
          Real-time network performance metrics
        </p>
      </div>

      <div className="space-y-8">
        <Suspense fallback={<Skeleton className="h-48 w-full" />}>
          <StatsGrid />
        </Suspense>

        <Suspense fallback={<Skeleton className="h-80 w-full" />}>
          <TPSChart hours={24} />
        </Suspense>
      </div>
    </div>
  );
}

