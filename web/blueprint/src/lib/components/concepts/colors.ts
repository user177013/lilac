import type {OverallScore} from '$osmanthus';

export const scoreToColor: Record<OverallScore, string> = {
  not_good: 'text-red-600',
  ok: 'text-yellow-600',
  good: 'text-green-600',
  very_good: 'text-osmanthus-charcoal',
  great: 'text-osmanthus-charcoal'
};

export const scoreToText: Record<OverallScore, string> = {
  not_good: 'Not good',
  ok: 'OK',
  good: 'Good',
  very_good: 'Very good',
  great: 'Great'
};
