// import * as React from 'react';
// import { Typography, Button } from '@mui/material';
// import { TwitterHashtagButton } from 'react-twitter-embed';

// export default function Hashtag(props) {
//   return (
//     <div>
//       <Typography variant='caption' display='block'>
//         Trending
//       </Typography>
//       <Typography variant='subtitle1'>
//         <b>{props.hashtag}</b>
//       </Typography>
//       <Typography variant='caption'>
//         <b>{props.count}</b>
//       </Typography>
//     </div>
//   );
// }

import * as React from 'react';
import { Typography, Button } from '@mui/material';
import { TwitterHashtagButton } from 'react-twitter-embed';

export default function Hashtag(props) {
  return (
    <div  >
      <Typography variant='body1' display='inline'>
        <b>{props.hashtag}  </b>
      </Typography>
      <Typography variant='caption'>{props.count}</Typography> 
    </div>
  );
}