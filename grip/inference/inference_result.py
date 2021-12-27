#  This software is Copyright (c) 2015 The Regents of the University of
#  California. All Rights Reserved. Permission to copy, modify, and distribute this
#  software and its documentation for academic research and education purposes,
#  without fee, and without a written agreement is hereby granted, provided that
#  the above copyright notice, this paragraph and the following three paragraphs
#  appear in all copies. Permission to make use of this software for other than
#  academic research and education purposes may be obtained by contacting:
#
#  Office of Innovation and Commercialization
#  9500 Gilman Drive, Mail Code 0910
#  University of California
#  La Jolla, CA 92093-0910
#  (858) 534-5815
#  invent@ucsd.edu
#
#  This software program and documentation are copyrighted by The Regents of the
#  University of California. The software program and documentation are supplied
#  "as is", without any accompanying services from The Regents. The Regents does
#  not warrant that the operation of the program will be uninterrupted or
#  error-free. The end-user understands that the program was developed for research
#  purposes and is advised not to rely exclusively on the program for any reason.
#
#  IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
#  DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST
#  PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
#  THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH
#  DAMAGE. THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
#  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS
#  IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
#  MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

import grip.inference


class InferenceResult:
    def __init__(self, inferences):
        """
        Save inferences to InferenceResults.

        :param inferences:
        """
        # make sure inferences is a list and has at least one Inference object
        assert all([isinstance(inference, grip.inference.Inference) for inference in inferences])

        # sorted list of inferences (see inference.py for sorting details)
        self.inferences = sorted(inferences, reverse=True)
        # get the highest-sorted inference as the primary inference
        if inferences:
            self.primary_inference = self.inferences[0]
        else:
            self.primary_inference = None

    def get_primary_suspicion(self):
        if self.primary_inference:
            return self.primary_inference.suspicion_level
        return None

    def as_dict(self):
        """
        Export to dict
        :return:
        """
        if self.primary_inference:
            primary_inference = self.primary_inference.as_dict()
        else:
            primary_inference = None
        return {
            "inferences": [inference.as_dict() for inference in self.inferences],
            "primary_inference": primary_inference,
        }

    @staticmethod
    def from_dict(inference_dict):
        new_dict = {}
        try:
            for field in ['inferences']:
                new_dict[field] = inference_dict.get(field, None)
        except KeyError:
            # it's ok/normal if the deprecated keys are not in
            pass

        return InferenceResult(**new_dict)
