import Link from 'next/link';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Activity, Database, Zap, TrendingUp } from 'lucide-react';

export default function HomePage() {
  return (
    <div className="container py-12">
      <div className="flex flex-col items-center text-center mb-16">
        <h1 className="text-4xl font-bold tracking-tight sm:text-6xl mb-4">
          Solana Analytics Platform
        </h1>
        <p className="text-xl text-muted-foreground max-w-2xl mb-8">
          Real-time blockchain analytics powered by Jetstreamer. Track programs,
          monitor network health, and explore transactions.
        </p>
        <div className="flex gap-4">
          <Link href="/programs">
            <Button size="lg">Explore Programs</Button>
          </Link>
          <Link href="/network">
            <Button size="lg" variant="outline">
              Network Stats
            </Button>
          </Link>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <Card>
          <CardHeader>
            <Activity className="h-8 w-8 mb-2 text-primary" />
            <CardTitle>Program Analytics</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">
              Track the most active Solana programs with detailed transaction
              metrics and success rates.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <Zap className="h-8 w-8 mb-2 text-primary" />
            <CardTitle>Real-time TPS</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">
              Monitor network performance with live transactions per second and
              throughput metrics.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <Database className="h-8 w-8 mb-2 text-primary" />
            <CardTitle>Transaction Search</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">
              Search and analyze individual transactions with detailed compute
              unit usage.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <TrendingUp className="h-8 w-8 mb-2 text-primary" />
            <CardTitle>Historical Data</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">
              Access historical program activity and network trends over time.
            </p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

