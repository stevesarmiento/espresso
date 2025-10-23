import Link from 'next/link';
import { formatNumber, formatComputeUnits, formatProgramId } from '@/lib/utils/format';
import type { ProgramStats } from '@/types';
import { ArrowUpRight } from 'lucide-react';

interface ProgramCardProps {
  program: ProgramStats;
}

export function ProgramCard({ program }: ProgramCardProps) {
  const successRate = program.success_rate;
  const successColor = 
    successRate >= 95 ? 'text-green-600' : 
    successRate >= 80 ? 'text-yellow-600' : 
    'text-red-600';

  return (
    <Link href={`/programs/${program.program_id}`}>
      <div className="group relative p-6 border border-border-low bg-white hover:shadow-lg transition-all duration-200 cursor-pointer h-full">
        <div className="product-card-diagonal absolute inset-0 pointer-events-none" />
        
        <div className="relative z-10">
          <div className="flex items-center justify-between mb-6">
            <code className="text-sm font-berkeley-mono text-gray-900">
              {formatProgramId(program.program_id)}
            </code>
            <ArrowUpRight className="h-4 w-4 text-gray-400 group-hover:text-gray-900 transition-colors" />
          </div>
          
          <div className="space-y-4">
            <div className="flex justify-between items-center">
              <span className="text-body-md text-gray-600">Transactions</span>
              <span className="font-medium text-gray-900">
                {formatNumber(program.total_transactions, 0)}
              </span>
            </div>
            
            <div className="flex justify-between items-center">
              <span className="text-body-md text-gray-600">Success Rate</span>
              <span className={`font-medium ${successColor}`}>
                {program.success_rate.toFixed(1)}%
              </span>
            </div>
            
            <div className="flex justify-between items-center">
              <span className="text-body-md text-gray-600">Avg CU</span>
              <span className="text-body-md text-gray-900">
                {formatComputeUnits(program.avg_compute_units)}
              </span>
            </div>
            
            {program.total_errors > 0 && (
              <div className="flex justify-between items-center">
                <span className="text-body-md text-gray-600">Errors</span>
                <span className="text-body-md text-red-600">
                  {formatNumber(program.total_errors, 0)}
                </span>
              </div>
            )}
          </div>
        </div>
      </div>
    </Link>
  );
}

